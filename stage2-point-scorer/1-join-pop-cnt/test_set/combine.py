# spark-submit --conf spark.yarn.maxAppAttempts=1 combine.py 
# nohup spark-submit --conf spark.yarn.maxAppAttempts=1 combine.py > run.log 2>&1 & 

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("bdad_proj/test_partial_points_pop_10d")
df.createOrReplaceTempView("points")

result = spark.sql("""
        SELECT lon, lat, n, d, score, building_cnt_1d, building_area_1d, building_cnt_2d, building_area_2d, building_cnt_3d, building_area_3d, building_cnt_6d, building_area_6d, building_cnt_10d, building_area_10d, building_cnt_20d, building_area_20d, building_cnt_40d, building_area_40d, elevRank_1d, elevRank_3d, elevRank_10d, elevRank_40d, pop_1d_sum, pop_3d_sum, SUM(pop_10d) AS pop_10d_sum
        FROM points
        GROUP BY points.lon, points.lat, n, d, score, building_cnt_1d, building_area_1d, building_cnt_2d, building_area_2d, building_cnt_3d, building_area_3d, building_cnt_6d, building_area_6d, building_cnt_10d, building_area_10d, building_cnt_20d, building_area_20d, building_cnt_40d, building_area_40d, elevRank_1d, elevRank_3d, elevRank_10d, elevRank_40d, pop_1d_sum, pop_3d_sum
    """)
    
result.write.format("parquet").save("bdad_proj/test_points_pop_10d")
