// sbt package
// spark-submit --packages "org.locationtech.geotrellis:geotrellis-spark_2.12:3.1.0" --class PopCntClean target/scala-2.12/popcntcleanapp_2.12-1.0.jar
// nohup spark-submit --packages "org.locationtech.geotrellis:geotrellis-spark_2.12:3.1.0" --class PopCntClean target/scala-2.12/popcntcleanapp_2.12-1.0.jar > run.log 2>&1 &
// yarn application -list -appStates RUNNING
// yarn application -kill application_1691775874963_33065

import org.apache.spark.sql._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.Path
import geotrellis.spark.store.hadoop.HadoopGeoTiffRDD
import geotrellis.raster._
import geotrellis.vector.{Extent,Point}

object PopCntClean {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                                .appName("PopCntClean")
                                .getOrCreate()
        val sc = spark.sparkContext 

        import spark.implicits._

		// file path on HDFS
        val inputTiff = new Path("/user/gw2310_nyu_edu/bdad_proj/pop_cnt_1km.tif")  
        val outputDir = "/user/gw2310_nyu_edu/bdad_proj/pop_cnt_clean"

		// use distributed io to read data on HDFS
		// the whole tile will be splited into several tiffRDD
        val geoTiffRDD = HadoopGeoTiffRDD.spatial(inputTiff)(sc)

        // create a rectangle determined by (42 lat, -71 lon) and (33 lat, -125 lon)
        val filterExtent = Extent(-125, 33, -71, 42)

        // first filter: keep tileRdd intersected with the research scope
		// some intersected tileRdds may contain points outside research scope
        val filteredRDD = geoTiffRDD.filter { case (key, tile) =>
            filterExtent.intersects(key.extent)
        }

        // second filter: for each tileRDD, get valid point as a row in rdd, which avoids converting in the last step
        val records = filteredRDD.flatMap { case (key, tile) =>
            val rasterExtent = RasterExtent(key.extent, tile.cols, tile.rows)
            for {
                col <- 0 until tile.cols
                row <- 0 until tile.rows
                pop_cnt = tile.getDouble(col, row)
                if !pop_cnt.isNaN
                latlon = rasterExtent.gridToMap(col, row)
                point = Point(latlon._1, latlon._2)
                if filterExtent.contains(point)
            } yield {
                Row(latlon._1, latlon._2, pop_cnt)
            }
        }

        val schema = StructType(
            List(
                StructField("lon", DoubleType, false),
                StructField("lat", DoubleType, false),
                StructField("pop_cnt", DoubleType, false)
            )
        )
        val df1 = spark.createDataFrame(records, schema)

        df1.write.mode("overwrite").parquet(outputDir)

        spark.stop()
    }
}
