# spark-submit --conf spark.yarn.maxAppAttempts=1 join_pop_3d.py
# nohup spark-submit --conf spark.yarn.maxAppAttempts=1 join_pop_3d.py > run.log 2>&1 & 

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

points_file = "bdad_proj/test_points_pop_10d"
pop_file = "bdad_proj/pop_cnt_1km_test_set"
output_file = "bdad_proj/test_partial_points_pop_40d"

points_df = spark.read.parquet(points_file)
points_df.createOrReplaceTempView("points")

pop_df = spark.read.parquet(pop_file)
pop_df.createOrReplaceTempView("pop_table")

result = spark.sql("""
        SELECT points.*, SUM(pop_table.pop_cnt) AS pop_40d
        FROM points
        LEFT JOIN pop_table ON SQRT(POW(pop_table.lon - points.lon, 2) + POW(pop_table.lat - points.lat, 2)) < 0.4
        GROUP BY points.lon, points.lat, n, d, score, building_cnt_1d, building_area_1d, building_cnt_2d, building_area_2d, building_cnt_3d, building_area_3d, building_cnt_6d, building_area_6d, building_cnt_10d, building_area_10d, building_cnt_20d, building_area_20d, building_cnt_40d, building_area_40d, elevRank_1d, elevRank_3d, elevRank_10d, elevRank_40d, pop_1d_sum, pop_3d_sum, pop_10d_sum
    """)
    
result.write.format("parquet").save(output_file)
