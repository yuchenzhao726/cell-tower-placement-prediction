import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode

object JoinData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
                            .appName("ElevationRank")
                            .getOrCreate()
                           
    import spark.implicits._

    val sc = spark.sparkContext
    
    // Load the data
    val masterElevationDataFrame = spark.read.parquet("hdfs://nyu-dataproc-m/user/tw2883_nyu_edu/bdad_project/masterElevation/")
    val pointWithScoreDataFrame = spark.read.parquet("hdfs://nyu-dataproc-m/user/tw2883_nyu_edu/bdad_project/points_building_40d/")
    
    pointWithScoreDataFrame.createOrReplaceTempView("pointWithScoreDFtemp")
    masterElevationDataFrame.createOrReplaceTempView("masterElevDFtemp")
    
    val joinedData = spark.sql("""
          SELECT 
            p.*,
            collect_list(ARRAY(SQRT(POW(m.longitude - p.lon, 2) + POW(m.latitude - p.lat, 2)), m.elevation)) AS distElevFilterred
          FROM 
            pointWithScoreDFtemp p
            LEFT JOIN masterElevDFtemp m 
            ON SQRT(POW(m.longitude - p.lon, 2) + POW(m.latitude - p.lat, 2)) < 0.01
          GROUP BY 
            p.lon, p.lat, p.n, p.d, p.score, 
            p.building_cnt_1d, p.building_area_1d, p.building_cnt_2d, p.building_area_2d, p.building_cnt_3d, p.building_area_3d, p.building_cnt_6d, p.building_area_6d, 
            p.building_cnt_10d, p.building_area_10d, p.building_cnt_20d, p.building_area_20d, p.building_cnt_40d, p.building_area_40d,
            p.elevRank_1d, p.elevRank_3d, p.elevrank_10d
        """)


    val explodedData = joinedData.withColumn("explodedDistElev", explode_outer(col("distElevFilterred"))).select(
      col("lon"),
      col("lat"),
      col("n"),
      col("d"),
      col("score"),
      col("pop_1d_sum"),
      col("pop_3d_sum"),
      col("pop_10d_sum"),
      col("pop_40d_sum"),
      col("building_cnt_1d"),
      col("building_area_1d"),
      col("building_cnt_2d"),
      col("building_area_2d"),
      col("building_cnt_3d"),
      col("building_area_3d"),
      col("building_cnt_6d"),
      col("building_area_6d"),
      col("building_cnt_10d"),
      col("building_area_10d"),
      col("building_cnt_20d"),
      col("building_area_20d"),
      col("building_cnt_40d"),
      col("building_area_40d"),
      col("elevRank_1d"),
      col("elevRank_3d"),
      col("elevRank_10d"),
      col("explodedDistElev").getItem(0).alias("distFilterred"),
      col("explodedDistElev").getItem(1).alias("elevFilterred")
    )

    // Count the percentRank of the elevationData
    val windowSpec = Window.partitionBy("lon", "lat").orderBy("elevFilterred")
    val percentRankData = explodedData.withColumn("elevRank_40d", percent_rank().over(windowSpec)) // "elevRank_1d", "elevRank_3d", "elevRank_10d", "elevRank_40d"
    
    // Extract the minimum dist num since the lon and lat may not perfectly match to the count_with_score lon and lat.
    val windowSpec = Window.partitionBy("lon", "lat").orderBy(col("distFilterred"))
    val minDistData = percentRankData.withColumn("row_num", row_number().over(windowSpec)).filter("row_num = 1").drop("row_num")
    
    // Drop distFilterred and elevFilterred
    val finalResultData = minDistData.drop("distFilterred", "elevFilterred")

    // Save to HDFS (training)
    //finalResultData.write.mode(SaveMode.Overwrite).parquet("hdfs://nyu-dataproc-m/user/tw2883_nyu_edu/bdad_project/elevationRank/elevRank_40d") // "elevRank_1d", "elevRank_3d", "elevRank_10d", "elevRank_40d"
    
    // Save to HDFS (test)
    finalResultData.write.mode(SaveMode.Overwrite).parquet("hdfs://nyu-dataproc-m/user/tw2883_nyu_edu/bdad_project/test/joinPointsWithScores_elevRank_40d")

    spark.stop()
  }
}
