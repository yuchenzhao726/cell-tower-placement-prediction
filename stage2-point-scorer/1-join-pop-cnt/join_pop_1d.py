# spark-submit --conf spark.yarn.maxAppAttempts=1 join_pop_1d.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

points_df = spark.read.parquet("bdad_proj/points_with_score")
points_df.createOrReplaceTempView("points")

pop_df = spark.read.parquet("bdad_proj/pop_cnt_1km")
pop_df.createOrReplaceTempView("pop_table")

result = spark.sql("""
        SELECT points.*, SUM(pop_table.pop_cnt) AS pop_1d
        FROM points
        LEFT JOIN pop_table ON SQRT(POW(pop_table.lon - points.lon, 2) + POW(pop_table.lat - points.lat, 2)) < 0.4
        GROUP BY points.lon, points.lat, points.n, points.d, points.score
    """)
    
result.write.format("parquet").save("bdad_proj/points_with_pop_1d")