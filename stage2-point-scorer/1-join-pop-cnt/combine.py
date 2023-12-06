# spark-submit --conf spark.yarn.maxAppAttempts=1 combine.py 
# nohup spark-submit --conf spark.yarn.maxAppAttempts=1 combine.py > run.log 2>&1 & 

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("bdad_project/partial_points_1d")
df.createOrReplaceTempView("points")

result = spark.sql("""
        SELECT lon, lat, n, d, score, SUM(pop_1d) AS pop_1d_sum
        FROM points
        GROUP BY lon, lat, n, d, score
    """)
    
result.write.format("parquet").save("bdad_proj/points_pop_1d")
