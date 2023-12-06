# spark-submit --conf spark.yarn.maxAppAttempts=1 join_pop_1d.py
# spark-submit --conf spark.yarn.maxAppAttempts=1 join_pop_1d.py
# nohup spark-submit --conf spark.yarn.maxAppAttempts=1 join_pop_1d.py > run.log 2>&1 & 

from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.getOrCreate()

points_file = sys.argv[1]
pop_file = sys.argv[2]
output_file = sys.argv[3]

points_df = spark.read.parquet(points_file)
points_df.createOrReplaceTempView("points")

pop_df = spark.read.parquet(pop_file)
pop_df.createOrReplaceTempView("pop_table")

result = spark.sql("""
        SELECT points.*, SUM(pop_table.pop_cnt) AS pop_1d
        FROM points
        LEFT JOIN pop_table ON SQRT(POW(pop_table.lon - points.lon, 2) + POW(pop_table.lat - points.lat, 2)) < 0.01
        GROUP BY points.lon, points.lat, points.n, points.d, points.score
    """)
    
result.write.format("parquet").mode("append").save(output_file)
