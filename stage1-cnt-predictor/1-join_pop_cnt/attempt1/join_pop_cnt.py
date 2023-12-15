from pyspark.sql import SparkSession

POP_FILE_PATH = "/user/gw2310_nyu_edu/bdad_proj/pop_cnt_clean"
CELL_FILE_PATH = "/user/gw2310_nyu_edu/bdad_proj/training_set_1"
SAVE_FILE_PATH = "/user/gw2310_nyu_edu/bdad_proj/training_set_test"
spark = SparkSession.builder.getOrCreate()

df0 = spark.read.parquet(POP_FILE_PATH)
df0.createOrReplaceTempView("pop_table")

df1 = spark.read.parquet(CELL_FILE_PATH)
df1.createOrReplaceTempView("random_places")

result = spark.sql("""
            SELECT random_places.*, SUM(pop_table.pop_cnt) AS pop_cnt_sum
            FROM random_places 
            LEFT JOIN pop_table ON pop_table.lon > random_places.lon1 AND pop_table.lon < random_places.lon2 AND pop_table.lat > random_places.lat1 AND pop_table.lat < random_places.lat2 
            GROUP BY random_places.lon1, random_places.lat1, random_places.lon2, random_places.lat2, random_places.area, random_places.cell_tower_num
        """) 

result.write.format("parquet").save(SAVE_FILE_PATH)

