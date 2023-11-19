# spark-submit --conf spark.yarn.maxAppAttempts=1 training_set_generator.py

from pyspark.sql import SparkSession
import random
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, DoubleType

SOURCE_FILE_PATH = "/user/yz8759_nyu_edu/project/datasets/cell_tower_us_lte_clustered"
SAVE_FILE_PATH = "/user/yz8759_nyu_edu/project/datasets/training_set_1"
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet(SOURCE_FILE_PATH)

df.createOrReplaceTempView("cell_tower")

# Training set is in the rectangle of (42 lat, -71 lon) and (33 lat, -125 lon)
random_places_list = []
i = 0
while i < 100000:
    rand_lon_1 = -125 + (-71 + 125) * random.random()
    rand_lat_1 = 33 + (42 - 33) * random.random()
    rand_lon_2 = -125 + (-71 + 125) * random.random()
    rand_lat_2 = 33 + (42 - 33) * random.random()
    if rand_lon_1 == rand_lon_2 or rand_lat_1 == rand_lat_2:
        continue
    elif abs(rand_lon_2 - rand_lon_1) < 0.5 or abs(rand_lat_2 - rand_lat_1) < 0.5:
        continue
    elif abs(rand_lon_2 - rand_lon_1) > 10 or abs(rand_lat_2 - rand_lat_1) > 10:
        continue
    random_places_list.append((min(rand_lon_1, rand_lon_2), 
                               min(rand_lat_1, rand_lat_2),
                               max(rand_lon_1, rand_lon_2), 
                               max(rand_lat_1, rand_lat_2)))
    i += 1

def area_calculator(lon1, lat1, lon2, lat2):
    length_of_a_degree_lon = {33: 58.07, 
                              34: 57.41, 
                              35: 56.72, 
                              36: 56.02, 
                              37: 55.31, 
                              38: 54.58, 
                              39: 53.83, 
                              40: 53.07, 
                              41: 52.28, 
                              42: 51.38, 
                              46: 48.13, 
                              47: 47.26, 
                              48: 46.37}
    length_of_a_degree_lat = 69
    return abs(lon1 - lon2) * length_of_a_degree_lon[abs(int(((lat1 - lat2) / 2.0) + lat2))] * abs(lat1 - lat2) * length_of_a_degree_lat

area_calculator = udf(area_calculator, DoubleType())

schema = "lon1 DOUBLE, lat1 DOUBLE, lon2 DOUBLE, lat2 DOUBLE"
random_places_df = spark.createDataFrame(random_places_list, schema=schema)

# calculate the area of each rectangle and add it to the dataframe
random_places_with_area_df = random_places_df.select(random_places_df["lon1"], 
                                                     random_places_df["lat1"], 
                                                     random_places_df["lon2"], 
                                                     random_places_df["lat2"], 
                                                     area_calculator(random_places_df["lon1"], 
                                                                     random_places_df["lat1"], 
                                                                     random_places_df["lon2"], 
                                                                     random_places_df["lat2"]).alias("area"))
random_places_with_area_df.createOrReplaceTempView("random_places")

# for each record in random_places, count the number of cell towers in the rectangle
result = spark.sql("SELECT random_places.*, COUNT(cell_tower.*) AS cell_tower_num \
                    FROM random_places \
                    LEFT JOIN cell_tower ON cell_tower.lon > random_places.lon1 AND cell_tower.lon < random_places.lon2 AND cell_tower.lat > random_places.lat1 AND cell_tower.lat < random_places.lat2 \
                    GROUP BY random_places.lon1, random_places.lat1, random_places.lon2, random_places.lat2, random_places.area") 


result.write.format("parquet").mode("overwrite").save(SAVE_FILE_PATH)