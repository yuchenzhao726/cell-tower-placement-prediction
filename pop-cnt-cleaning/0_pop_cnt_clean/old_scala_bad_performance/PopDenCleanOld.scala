// sbt package
// spark-submit --packages "org.locationtech.geotrellis:geotrellis-spark_2.12:3.1.0" --class PopDenClean target/scala-2.12/popdencleanapp_2.12-1.0.jar
// nohup spark-submit --packages "org.locationtech.geotrellis:geotrellis-spark_2.12:3.1.0" --class PopDenClean target/scala-2.12/popdencleanapp_2.12-1.0.jar > log 2>&1 &
// yarn application -list -appStates RUNNING
// yarn application -kill application_1691775874963_33065

import org.apache.spark.sql._
import org.apache.hadoop.fs.Path
import geotrellis.spark.store.hadoop.HadoopGeoTiffReader
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.vector.Extent
import geotrellis.vector.Point

object PopDenClean {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                                .appName("PopDenClean")
                                .getOrCreate()
        val sc = spark.sparkContext 

        import spark.implicits._

        val inputTiff = new Path("/user/gw2310_nyu_edu/bdad_proj/population.tif")  
        val inputDataDir = "/user/gw2310_nyu_edu/bdad_proj/training_set_1" 
        val outputDir = "/user/gw2310_nyu_edu/bdad_proj/training_set_2"

        val geoTiff: SinglebandGeoTiff = HadoopGeoTiffReader.readSingleband(inputTiff)(sc)
        val tile = geoTiff.tile

        // create a rectangle within (42 lat, -71 lon) and (33 lat, -125 lon)
        val filterExtent = Extent(-125, 33, -71, 42) 

        // filter in valid data in the rectangle
        val records = for {
            col <- 0 until tile.cols
            row <- 0 until tile.rows
            latlon = geoTiff.rasterExtent.gridToMap(col, row)
            if filterExtent.contains(Point(latlon._1, latlon._2))
            pop_den = tile.getDouble(col, row)
            if !pop_den.isNaN
        } yield {
            (latlon._1, latlon._2, pop_den)
        }

        val df0 = records.toSeq.toDF("lon", "lat", "pop_den")
        df0.createOrReplaceTempView("pop_table_un")

        // normalize data between 0 and 1 by x-min/max-min
        // partition is needed for efficiency
        val df1 = spark.sql("""
            SELECT lon, lat, pop_den / max_pop_den AS pop_den 
            FROM ( 
                SELECT lon, lat, pop_den, MAX(pop_den) OVER (PARTITION BY (lon, lat)) as max_pop_den 
                FROM pop_table_un 
            ) 
        """)
        df1.createOrReplaceTempView("pop_table")

        // join pop_den to random_places
        val df2 = spark.read.parquet(inputDataDir)
        df2.createOrReplaceTempView("random_places")

        val result = spark.sql("""
            SELECT random_places.*, AVG(pop_table.pop_den) AS pop_den 
            FROM random_places 
            LEFT JOIN pop_table ON pop_table.lon > random_places.lon1 AND pop_table.lon < random_places.lon2 AND pop_table.lat > random_places.lat1 AND pop_table.lat < random_places.lat2 
            GROUP BY random_places.lon1, random_places.lat1, random_places.lon2, random_places.lat2, random_places.area, random_places.cell_tower_num
        """) 

        result.write.parquet(outputDir)
        
        spark.stop()
    }
}