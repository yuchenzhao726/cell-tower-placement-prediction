// sbt package
// spark-submit --class JoinPopDenApp target/scala-2.12/joinpopdenapp_2.12-1.0.jar
// nohup spark-submit --class JoinPopDenApp target/scala-2.12/joinpopdenapp_2.12-1.0.jar > run.log 2>&1 &
// yarn application -list -appStates RUNNING
// yarn application -kill application_1691775874963_33065

import org.apache.spark.sql._

object JoinPopDenApp {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                                .appName("JoinPopDen")
                                .getOrCreate()

        val popInputDir = "/user/gw2310_nyu_edu/bdad_proj/pop_den_clean" 
        val towerInputDir = "/user/gw2310_nyu_edu/bdad_proj/training_set_1" 
        val outputDir = "/user/gw2310_nyu_edu/bdad_proj/training_set_2"

        val df1 = spark.read.parquet(popInputDir)
        df1.createOrReplaceTempView("pop_table")

        val df2 = spark.read.parquet(towerInputDir)
        df2.createOrReplaceTempView("random_places")

        val result = spark.sql("""
            SELECT random_places.*, AVG(pop_table.pop_den) AS pop_den 
            FROM random_places 
            LEFT JOIN pop_table ON pop_table.lon > random_places.lon1 AND pop_table.lon < random_places.lon2 AND pop_table.lat > random_places.lat1 AND pop_table.lat < random_places.lat2 
            GROUP BY random_places.lon1, random_places.lat1, random_places.lon2, random_places.lat2, random_places.area, random_places.cell_tower_num
        """) 

        result.write.parquet(outputDir)

        spark.stop()
    }
}
