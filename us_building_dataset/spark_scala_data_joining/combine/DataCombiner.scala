// sbt package
// spark-submit --class DataCombiner target/scala-2.12/combine-building-data_2.12-1.0.jar $INPUT_DIR $PARTIAL_JOIN_OUTPUT $COMBINED_OUTPUT

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DataCombiner {
    def main(args: Array[String]): Unit = {
        if (args.length < 3) {
            System.err.println("Usage: DataCombiner <input-directory1> <input-directory2> <output-directory>")
            System.exit(1)
        }
        val spark = SparkSession.builder().appName("DataCombiner").getOrCreate()

        // Dataframe containing the original random area 
        var df1 = spark.read.format("parquet").load(args(0))
        df1.createOrReplaceTempView("table1")

        // Dataframe containing the newly generated building dataset
        val df2 = spark.read.format("parquet").load(args(1))
        df2.groupBy("lon1", "lat1", "lon2", "lat2")
            .agg(
                sum("building_count").alias("total_building_cnt"), 
                sum("building_area").alias("total_building_area"))
            .createOrReplaceTempView("table2")

        val conbimedDF = spark.sql("""
            SELECT 
                table1.*,
                table2.total_building_cnt, 
                table2.total_building_area
            FROM 
                table1
            JOIN 
                table2 
            ON 
               table1.lon1 == table2.lon1
               AND table1.lat1 == table2.lat1
               AND table1.lon2 == table2.lon2
               AND table1.lat2 == table2.lat2
        """)

        conbimedDF.write.format("parquet").mode("overwrite").save(args(2))
    }
}