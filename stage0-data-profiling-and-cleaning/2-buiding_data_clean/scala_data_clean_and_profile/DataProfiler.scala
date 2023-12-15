// sbt package
// spark-submit --class DataProfiler target/scala-2.12/building-data-profiler_2.12-1.0.jar bdad_project/open_address_train_dataset bdad_project/partitioned_train_dataset

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataProfiler {
    def main(args: Array[String]): Unit = {
        val OUTPUT_FILE_COUNT = 96
        if (args.length < 2) {
            System.err.println("Usage: BuildingDataProfiler <input-directory> <output-directory>")
            System.exit(1)
        }
        val spark = SparkSession.builder().appName("ProfileBuildingData").getOrCreate()

        val df = spark.read.format("parquet").load(args(0))
        
        // Display the data distribution
        df.printSchema()
        df.describe().show()

        // Filter out buildings with zero area size
        // OR area size > 500K square meters
        val zeroAreaBuildingCount = df.filter(col("area") === 0).count()
        println(s"No. of Buildings with 0 area: $zeroAreaBuildingCount")
        val largeAreaBuildingCount = df.filter(col("area") >= 500000).count()
        println(s"No. of Buildings with area larger than 500K sqm: $largeAreaBuildingCount")

        val result = df.filter((col("area") =!= 0) && (col("area") < 500000))
        result.describe().show()

        // Repartition the DataFrame to control the number of output files
        val repartitionedDF = result.repartition(OUTPUT_FILE_COUNT)

        repartitionedDF.write.format("parquet").mode("overwrite").save(args(1))
    }
}
