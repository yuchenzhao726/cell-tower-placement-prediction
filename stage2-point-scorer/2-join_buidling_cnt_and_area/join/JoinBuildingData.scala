// sbt package

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object JoinBuildingData {
    def main(args: Array[String]): Unit = {
        if (args.length < 3) {
            System.err.println("Usage: JoinBuildingData <existing-dataset> <buildings-to-join> <output-directory>")
            System.exit(1)
        }
        val spark = SparkSession.builder().appName("JoinBuildingData").getOrCreate()

        // Dataframe containing the random points selected for training
        val originalDF = spark.read.format("parquet").load(args(0))
        originalDF.createOrReplaceTempView("locations")

        // Dataframe containing the building information to be joined 
        val buildingDF = spark.read.format("parquet").load(args(1))
        buildingDF.createOrReplaceTempView("buildings")

        // Define the UDF to calculate the distance from the building to the given point and save the result to [distance, area]
        val distanceCalc = (lon1: Double, lat1: Double, lon2: Double, lat2: Double, area: Double) => {
            List(Math.sqrt(Math.pow(lon1 - lon2, 2) + Math.pow(lat1 - lat2, 2)), area)
        }

        spark.udf.register("distanceCalc", distanceCalc)

        val result = spark.sql("""
            SELECT
                t1.lon,
                t1.lat,
                COLLECT_LIST(distanceCalc(t1.lon, t1.lat, t2.center_longitude, t2.center_latitude, t2.area)) AS building_list
            FROM
                locations t1
            LEFT JOIN
                buildings t2 ON sqrt(pow(t1.lon - t2.center_longitude, 2) + pow(t1.lat - t2.center_latitude, 2)) < 0.4
            GROUP BY
                t1.lon, t1.lat
        """)

        result.write.format("parquet").mode("append").save(args(2))
    }
}
