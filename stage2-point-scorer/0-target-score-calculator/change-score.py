from pyspark.sql import SparkSession
import math
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, DoubleType

spark = SparkSession.builder.getOrCreate()
schema = "lon DOUBLE, lat DOUBLE, n INT, d DOUBLE, score DOUBLE"
df = spark.read.parquet("/user/yz8759_nyu_edu/project/datasets/points_with_score").drop("score")
df.createOrReplaceTempView("df_view")

def score_calculator(n, d):
    if n is None:
        n = 0
    if d is None:
        return 0.5 * (1 - 1 / math.pow(math.log(n + 3, 3), 2))
    return 1 / (2 + 67 * d) + 0.5 * (1 - 1 / math.pow(math.log(n + 40, 40), 2))


spark.udf.register("score_calculator", score_calculator, DoubleType())


result = spark.sql("""
                    SELECT lon, lat, n, d, score_calculator(n, d) AS score 
                    FROM df_view
                    """)
result.write.format("parquet").mode("overwrite").save("/user/yz8759_nyu_edu/project/datasets/points_with_score_2")