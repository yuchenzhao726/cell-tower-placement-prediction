# spark-submit --conf spark.yarn.maxAppAttempts=1 repartition.py

from pyspark.sql import SparkSession

POP_FILE_PATH = "/user/gw2310_nyu_edu/bdad_proj/pop_cnt_clean"
CELL_FILE_PATH = "/user/gw2310_nyu_edu/bdad_proj/training_set_1"
POP_FILE_SAVE = "/user/gw2310_nyu_edu/bdad_proj/pop_cnt"
CELL_FILE_SAVE = "/user/gw2310_nyu_edu/bdad_proj/cell_tower"

spark = SparkSession.builder.getOrCreate()

df0 = spark.read.parquet(CELL_FILE_PATH)
df0 = df0.repartition(16, "lon1", "lon2", "lat1", "lat2")
df0.write.format("parquet").mode("overwrite").save(CELL_FILE_SAVE)

df1 = spark.read.parquet(POP_FILE_PATH)
df1 = df1.repartition(16, "lon", "lat")
df1.write.format("parquet").mode("overwrite").save(POP_FILE_SAVE)
