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
        val df1 = spark.read.format("parquet").load(args(0))
        df1.createOrReplaceTempView("table1")

        // Dataframe containing the newly generated building dataset
        val df2 = spark.read.format("parquet").load(args(1))
        df2.createOrReplaceTempView("table2")

        val combinedDF = spark.sql("""
            SELECT 
                table1.*,
                table2.building_cnt_1d, 
                table2.building_area_1d,
                table2.building_cnt_2d, 
                table2.building_area_2d,
                table2.building_cnt_3d, 
                table2.building_area_3d,
                table2.building_cnt_6d, 
                table2.building_area_6d,
                table2.building_cnt_10d, 
                table2.building_area_10d,
                table2.building_cnt_20d, 
                table2.building_area_20d,
                table2.building_cnt_40d, 
                table2.building_area_40d
            FROM 
                table1
            JOIN 
                table2 
            ON 
               table1.lon == table2.lon
               AND table1.lat == table2.lat
        """)

        combinedDF.write.format("parquet").mode("overwrite").save(args(2))
    }
}