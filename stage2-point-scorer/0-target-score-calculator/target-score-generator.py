from pyspark.sql import SparkSession
import math
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, DoubleType

spark = SparkSession.builder.getOrCreate()
ct_df = spark.read.parquet("/user/yz8759_nyu_edu/project/datasets/cell_tower_us_lte_clustered")

ct_df.createOrReplaceTempView("cell_tower")

# Data range is in the rectangle of (42 lat, -71 lon) and (33 lat, -125 lon)
# Generate points in the rectangle, 0.04 degree between two points
# 0.04 degree is about 4.4km
points_list = [(-125+i*0.04, 33+j*0.04) for i in range(25*(125-71)) for j in range(25*(42-33))]

schema = "lon DOUBLE, lat DOUBLE"
points_df = spark.createDataFrame(points_list, schema=schema)
points_df.createOrReplaceTempView("points")

# for point (lon, lat), 
# d = the nearest cell_tower's distance to the point
# n = the number of cell_towers within 0.4 square degrees of the point
# score = 1 / (2 + 20 * d) + 0.5 * (1 - 1 / log_3^(n + 3))
def score_calculator(n, d):
    if n is None:
        n = 0
    if d is None:
        return 0.5 * (1 - 1 / math.pow(math.log(n + 40, 40), 2))
    return 1 / (2 + 67 * d) + 0.5 * (1 - 1 / math.pow(math.log(n + 40, 40), 2))

spark.udf.register("score_calculator", score_calculator, DoubleType())

# SELECT points.lon AS lon, points.lat AS lat, COUNT(*) AS n, MIN(distance(cell_tower.lon, cell_tower.lat, points.lon, points.lat)) AS d
points_adv = spark.sql("""
                    SELECT points.lon AS lon, points.lat AS lat, COUNT(*) - 1 AS n, MIN(SQRT(POW(cell_tower.lon - points.lon, 2) + POW(cell_tower.lat - points.lat, 2))) AS d
                    FROM points
                    LEFT JOIN cell_tower ON SQRT(POW(cell_tower.lon - points.lon, 2) + POW(cell_tower.lat - points.lat, 2)) < 0.4
                    GROUP BY points.lon, points.lat
                  """)

points_adv.createOrReplaceTempView("points_with_n_and_d")

result = spark.sql("""
                    SELECT lon, lat, n, d, score_calculator(n, d) AS score
                    FROM points_with_n_and_d
                    """)

result.write.format("parquet").mode("overwrite").save("/user/yz8759_nyu_edu/project/datasets/points_with_score")



#################
# SELECT points.lon AS lon, points.lat AS lat, COUNT(*) AS n, MIN(distance(cell_tower.lon, cell_tower.lat, points.lon, points.lat)) AS d
# FROM points
# LEFT JOIN cell_tower ON distance(cell_tower.lon, cell_tower.lat, points.lon, points.lat) < 0.4
# GROUP BY points.lon, points.lat
# 
# SELECT lon, lat score_calculator(n, d) AS score
# FROM points_with_n_and_d