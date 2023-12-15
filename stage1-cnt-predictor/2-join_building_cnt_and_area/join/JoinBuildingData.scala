// sbt package

import org.apache.spark.sql._

object JoinBuildingData {
    def main(args: Array[String]): Unit = {
        if (args.length < 3) {
            System.err.println("Usage: JoinBuildingData <existing-dataset> <buildings-to-join> <output-directory>")
            System.exit(1)
        }
        val spark = SparkSession.builder().appName("JoinBuildingData").getOrCreate()

        // Dataframe containing the random areas selected for training
        val df_original = spark.read.format("parquet").load(args(0))
        df_original.createOrReplaceTempView("locations")

        // Dataframe containing the building information to be joined 
        val df_building = spark.read.format("parquet").load(args(1))
        df_building.createOrReplaceTempView("buildings")

        val result = spark.sql("""
            SELECT 
                locations.lon1, 
                locations.lat1, 
                locations.lon2, 
                locations.lat2, 
                COUNT(buildings.area) AS building_count, 
                SUM(buildings.area) AS building_area
            FROM 
                locations
            LEFT JOIN 
                buildings 
            ON 
                buildings.center_longitude > locations.lon1 
                AND buildings.center_longitude < locations.lon2 
                AND buildings.center_latitude > locations.lat1 
                AND buildings.center_latitude < locations.lat2
            GROUP BY 
                locations.lon1, 
                locations.lat1, 
                locations.lon2, 
                locations.lat2
        """)

        result.write.format("parquet").mode("append").save(args(2))
    }
}
