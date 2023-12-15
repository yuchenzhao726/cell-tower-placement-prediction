// sbt package
// spark-submit --class SplitToLowerDegree target/scala-2.12/split-building-data-to-lower-degree_2.12-1.0.jar $INPUT_DIR $OUTPUT_DIR

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SplitToLowerDegree {
    def main(args: Array[String]): Unit = {
        if (args.length < 2) {
            System.err.println("Usage: JoinBuildingData <input-directory> <output-directory>")
            System.exit(1)
        }
        val spark = SparkSession.builder().appName("JoinBuildingData").getOrCreate()

        // Dataframe containing the building list within 0.4 degree within the given point
        val df_40d = spark.read.format("parquet").load(args(0))
        df_40d.createOrReplaceTempView("building_40d")
        
        // Define UDF to calculate the total building count and area within specified degree
        val getBuildingInfo = (buildings: Seq[Seq[Double]], degree: Double) => {
            val filteredBuildings = buildings.filter(_(0) < degree)
            val totalCnt = filteredBuildings.size
            val totalArea = filteredBuildings.map(_(1)).sum
            (totalCnt, totalArea)
        }

        spark.udf.register("getBuildingInfo", getBuildingInfo)

        val partitioned_result = spark.sql("""
            SELECT lon, lat,
                getBuildingInfo(building_list, 0.01)._1 AS building_1d_cnt,
                getBuildingInfo(building_list, 0.01)._2 AS building_1d_area,
                getBuildingInfo(building_list, 0.02)._1 AS building_2d_cnt,
                getBuildingInfo(building_list, 0.02)._2 AS building_2d_area,
                getBuildingInfo(building_list, 0.03)._1 AS building_3d_cnt,
                getBuildingInfo(building_list, 0.03)._2 AS building_3d_area,
                getBuildingInfo(building_list, 0.06)._1 AS building_6d_cnt,
                getBuildingInfo(building_list, 0.06)._2 AS building_6d_area,
                getBuildingInfo(building_list, 0.1)._1 AS building_10d_cnt,
                getBuildingInfo(building_list, 0.1)._2 AS building_10d_area,
                getBuildingInfo(building_list, 0.2)._1 AS building_20d_cnt,
                getBuildingInfo(building_list, 0.2)._2 AS building_20d_area,
                getBuildingInfo(building_list, 0.4)._1 AS building_40d_cnt,
                getBuildingInfo(building_list, 0.4)._2 AS building_40d_area
            FROM 
                building_40d
        """)

        val result = partitioned_result.groupBy("lon", "lat")
            .agg(
                sum("building_1d_cnt").alias("building_cnt_1d"), 
                sum("building_1d_area").alias("building_area_1d"),
                sum("building_2d_cnt").alias("building_cnt_2d"), 
                sum("building_2d_area").alias("building_area_2d"),
                sum("building_3d_cnt").alias("building_cnt_3d"), 
                sum("building_3d_area").alias("building_area_3d"),
                sum("building_6d_cnt").alias("building_cnt_6d"), 
                sum("building_6d_area").alias("building_area_6d"),
                sum("building_10d_cnt").alias("building_cnt_10d"), 
                sum("building_10d_area").alias("building_area_10d"),
                sum("building_20d_cnt").alias("building_cnt_20d"), 
                sum("building_20d_area").alias("building_area_20d"),
                sum("building_40d_cnt").alias("building_cnt_40d"), 
                sum("building_40d_area").alias("building_area_40d"))

        result.write.format("parquet").mode("overwrite").save(args(1))
    }
}
