from pyspark.sql import SparkSession
import math
from pyspark.sql.types import StringType, DoubleType

spark = SparkSession.builder.getOrCreate()
ct_df = spark.read.parquet("/user/yz8759_nyu_edu/project/datasets/cell_tower_us_lte_clustered").na.fill(0)

ct_df.createOrReplaceTempView("cell_tower")

points_df = spark.read.option("header", True).option("inferSchema", True).csv("/user/yz8759_nyu_edu/project/datasets/stage2").drop("score").drop("n").drop("d").na.fill(0)
points_df.createOrReplaceTempView("points")

def add_score(lon1, lat1, lon2, lat2):
    if lon2 is None or lat2 is None:
        return 0
    distance = math.sqrt((lon1-lon2)**2 + (lat1-lat2)**2)
    return 1 / distance**2

spark.udf.register("add_score", add_score, DoubleType())

result_temp = spark.sql("""
                    SELECT points.lon AS points_lon, points.lat AS points_lat, SUM(add_score(points.lon, points.lat, cell_tower.lon, cell_tower.lat)) AS score
                    FROM points
                    LEFT JOIN cell_tower ON SQRT(POW(cell_tower.lon - points.lon, 2) + POW(cell_tower.lat - points.lat, 2)) < 0.4
                    GROUP BY points.lon, points.lat
                  """)
result_temp.createOrReplaceTempView("result_temp")

result = spark.sql("""
                    SELECT *
                    FROM result_temp
                    LEFT JOIN points ON result_temp.points_lon = points.lon AND result_temp.points_lat = points.lat
                   """)

result2 = result.drop("points_lon").drop("points_lat")

result2.write.format("parquet").mode("overwrite").save("/user/yz8759_nyu_edu/project/datasets/stage2-with-new-score")