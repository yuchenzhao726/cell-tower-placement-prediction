# pyspark --deploy-mode client
# nohup spark-submit --conf spark.yarn.maxAppAttempts=2 sharded_join_pop_cnt.py > sharded_join.log 2>&1 &

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,lit,col
from pyspark import StorageLevel
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.getOrCreate()

POP_FILE_PATH = "/user/gw2310_nyu_edu/bdad_proj/pop_cnt_1km"
CELL_FILE_PATH = "/user/gw2310_nyu_edu/bdad_proj/training_set_1"
SAVE_FILE_PATH = "/user/gw2310_nyu_edu/bdad_proj/sharded_join"

random_places_df = spark.read.parquet(CELL_FILE_PATH)
# .coalesce(16)

# random_places_df is loop invariant
random_places_df = random_places_df.withColumn('pop_cnt_sum', lit(0))

pop_table_df = spark.read.parquet(POP_FILE_PATH)
pop_table_parts = pop_table_df.randomSplit([1.0]*12)
pop_table_df = None

for part_df in pop_table_parts:
    part_df.persist(StorageLevel.DISK_ONLY)

for i, part_df in enumerate(pop_table_parts):
    joined_df = random_places_df.join(
        broadcast(part_df),
        (part_df.lon > random_places_df.lon1) &
        (part_df.lon < random_places_df.lon2) &
        (part_df.lat > random_places_df.lat1) &
        (part_df.lat < random_places_df.lat2),
        "left"
    ).groupBy('lon1', 'lat1', 'lon2', 'lat2','area','cell_tower_num','pop_cnt_sum').agg(sum('pop_cnt').alias('pop_part'))
    random_places_df = joined_df.withColumn("pop_cnt_sum", col("pop_cnt_sum") + col("pop_part")).drop("pop_part")
#   random_places_df = random_places_df.coalesce(16) 
    part_df.unpersist()
    pop_table_parts[i] = None

random_places_df.write.format("parquet").save(SAVE_FILE_PATH)    
