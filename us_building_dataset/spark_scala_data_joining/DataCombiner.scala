import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object DataCombiner {
    def main(args: Array[String]): Unit = {
        if (args.length < 3) {
            System.err.println("Usage: DataCombiner <input-directory1> <input-directory2> <output-directory>")
            System.exit(1)
        }
        val spark = SparkSession.builder().appName("DataCombiner").getOrCreate()

        // Initialize column building_count and building_area for join operation 
        var df1 = spark.read.format("parquet").load(args(0))
        if (!df1.columns.contains("building_count")) {
            df1 = df1.withColumn("building_count", lit(0))
        }
        if (!df1.columns.contains("building_area")) {
            df1 = df1.withColumn("building_area", lit(0))
        }
        df1.createOrReplaceTempView("table1")

        // Dataframe containing the newly generated 
        val df2 = spark.read.format("parquet").load(args(1))
        df2.createOrReplaceTempView("table2")

        val conbimedDF = spark.sql("""
            SELECT 
                table1.lon1,
                table1.lat1,
                table1.lon2,
                table1.lat2,
                table1.area,
                table1.cell_tower_num,
                table1.pop_cnt_sum_norm,
                table1.avg_elevation,
                table1.stddv_elevation,
                (table1.building_count + table2.building_count) AS building_count, 
                (table1.building_area + table2.building_area) AS building_area
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