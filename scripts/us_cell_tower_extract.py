# spark-submit --conf spark.yarn.maxAppAttempts=1 us_cell_tower_extract.py

from pyspark.sql import SparkSession

SOURCE_FILE_PATH = "/user/yz8759_nyu_edu/project/datasets/cell_tower_all.csv"
SAVE_FILE_PATH = "/user/yz8759_nyu_edu/project/datasets/cell_tower_us_lte_clustered"
spark = SparkSession.builder.getOrCreate()
schema = "radio STRING, mcc STRING, net STRING, area STRING, cell INT, unit STRING, lon DOUBLE, lat DOUBLE, range STRING, samples STRING, changeable STRING, created STRING, updated STRING, averageSignal STRING"
df = spark.read.csv(SOURCE_FILE_PATH, header=True, schema=schema)

df.createOrReplaceTempView("cell_tower")

result = spark.sql("SELECT FLOOR(cell / 256) AS bsid, AVG(lon) AS lon, AVG(lat) AS lat\
                   FROM cell_tower\
                   WHERE radio = 'LTE' AND (mcc = '310' OR mcc = '311' OR mcc = '312' OR mcc = '313' OR mcc = '314' OR mcc = '315' OR mcc = '316')\
                   GROUP BY bsid")

result.write.format("parquet").mode("overwrite").save(SAVE_FILE_PATH)
