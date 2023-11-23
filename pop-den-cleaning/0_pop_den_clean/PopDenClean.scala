// sbt package
// spark-submit --packages "org.locationtech.geotrellis:geotrellis-spark_2.12:3.1.0" --class PopDenCleanApp target/scala-2.12/popdencleanapp_2.12-1.0.jar
// nohup spark-submit --packages "org.locationtech.geotrellis:geotrellis-spark_2.12:3.1.0" --class PopDenCleanApp target/scala-2.12/popdencleanapp_2.12-1.0.jar > run.log 2>&1 &
// yarn application -list -appStates RUNNING
// yarn application -kill application_1691775874963_33065

import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.Path
import geotrellis.spark.store.hadoop.HadoopGeoTiffRDD
import geotrellis.raster._
import geotrellis.vector.Extent

object PopDenCleanApp {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                                .appName("PopDenClean")
                                .getOrCreate()
        val sc = spark.sparkContext 

        import spark.implicits._

        val inputTiff = new Path("/user/gw2310_nyu_edu/bdad_proj/population.tif")  
        val outputDir = "/user/gw2310_nyu_edu/bdad_proj/pop_den_clean"

        val geoTiffRDD = HadoopGeoTiffRDD.spatial(inputTiff)(sc)

        // create a rectangle within (42 lat, -71 lon) and (33 lat, -125 lon)
        val filterExtent = Extent(-125, 33, -71, 42)

        // filter in extent intersected with the rectangle
        val filteredRDD = geoTiffRDD.filter { case (key, tile) =>
            filterExtent.intersects(key.extent)
        }

        // filter in valid data in the rectangle
        val records = filteredRDD.flatMap { case (key, tile) =>
            val rasterExtent = RasterExtent(key.extent, tile.cols, tile.rows)
            for {
                col <- 0 until tile.cols
                row <- 0 until tile.rows
                pop_den = tile.getDouble(col, row)
                if !pop_den.isNaN
            } yield {
                val latlon = rasterExtent.gridToMap(col, row) 
                Row(latlon._1, latlon._2, pop_den)
            }
        }

        // normalize data between 0 and 1 by x-min/max-min
        val maxPopDen = records.map(row => row.getDouble(2)).max()
        val normalizedRecords = records.map(row => {
            Row(row.getDouble(0), row.getDouble(1), row.getDouble(2) / maxPopDen)
        })

        // create popDen dataframe
        val schema = StructType(
            List(
                StructField("lon", DoubleType, false),
                StructField("lat", DoubleType, false),
                StructField("pop_den", DoubleType, false)
            )
        )
        val df1 = spark.createDataFrame(normalizedRecords, schema)

        df1.write.parquet(outputDir)

        spark.stop()
    }
}
