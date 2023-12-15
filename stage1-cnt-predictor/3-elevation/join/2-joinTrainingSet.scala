import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode


object JoinData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
                            .appName("JoinData")
                            .getOrCreate()
                             

    import spark.implicits._

    val sc = spark.sparkContext
    
    // Load the data
    val masterElevationDataFrame = spark.read.parquet("hdfs://nyu-dataproc-m/user/tw2883_nyu_edu/bdad_project/masterElevation/")
    val trainingSetDataFrame = spark.read.parquet("hdfs://nyu-dataproc-m/user/tw2883_nyu_edu/bdad_project/training_set_2_n/")

    // Step 1: Join DataFrames and Select Columns
    val joinedDataFrame = trainingSetDataFrame.join(
      masterElevationDataFrame,
      col("longitude").between(col("lon1"), col("lon2")) &&
      col("latitude").between(col("lat1"), col("lat2"))
    ).select(
      col("lon1"),
      col("lat1"),
      col("lon2"),
      col("lat2"),
      col("area"),
      col("cell_tower_num"),
      col("pop_cnt_sum_norm"),
      col("elevation")
    )

    // Step 2: GroupBy and Aggregate
    val aggregatedDataFrame = joinedDataFrame.groupBy("lon1", "lat1", "lon2", "lat2", "area", "cell_tower_num","pop_cnt_sum_norm").agg(
      avg("elevation").as("avg_elevation"),
      stddev("elevation").as("stddv_elevation"),
      collect_list("elevation").as("filtered_elevations")
    )

    // Step 3: Fill Null Values
    val filledDataFrame = aggregatedDataFrame
      .withColumn("avg_elevation", when(col("avg_elevation").isNotNull, col("avg_elevation")).otherwise(lit(0)))
      .withColumn("stddv_elevation", when(col("stddv_elevation").isNotNull, col("stddv_elevation")).otherwise(lit(0)))


    // Step 4: Drop filtered_elevations Column
    val finalDataFrame = filledDataFrame.drop("filtered_elevations")

    
    // Step 5: Save completed training_set_1 to HDFS
    finalDataFrame.write.mode(SaveMode.Overwrite).parquet("hdfs://nyu-dataproc-m/user/tw2883_nyu_edu/bdad_project/training_set_3/")

    spark.stop()
  }
}

