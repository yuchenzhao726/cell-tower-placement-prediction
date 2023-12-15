# spark-submit --conf spark.yarn.maxAppAttempts=1 norm_pop_cnt.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, lit
# from pyspark.sql.window import Window

POP_FILE_PATH = "/user/gw2310_nyu_edu/bdad_proj/training_set_2"
POP_FILE_SAVE = "/user/gw2310_nyu_edu/bdad_proj/training_set_2_norm"
spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet(POP_FILE_PATH)


# window funtion method will cause repartition 
# windowSpec = Window.partitionBy() 
# newDf = df.withColumn("pop_cnt_sum_norm", col("pop_cnt_sum") / max("pop_cnt_sum").over(windowSpec)).drop("pop_cnt_sum")
max_value = df.select(max("pop_cnt_sum")).collect()[0][0]
# normalize data between 0 and 1 by x-min/max-min
newDf = df.withColumn("pop_cnt_sum_norm", col("pop_cnt_sum") / lit(max_value)).drop("pop_cnt_sum")

newDf.write.format("parquet").save(POP_FILE_SAVE)
