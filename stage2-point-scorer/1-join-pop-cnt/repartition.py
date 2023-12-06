# spark-submit --conf spark.yarn.maxAppAttempts=1 repartition.py

from pyspark.sql import SparkSession

FILE_PATH = "/user/gw2310_nyu_edu/bdad_proj/points_with_score"
FILE_SAVE = "/user/gw2310_nyu_edu/bdad_proj/points_with_score_repar"

spark = SparkSession.builder.getOrCreate()

df0 = spark.read.parquet(FILE_PATH)
df0 = df0.repartition(17, "lon", "lat")
df0.write.format("parquet").mode("overwrite").save(FILE_SAVE)