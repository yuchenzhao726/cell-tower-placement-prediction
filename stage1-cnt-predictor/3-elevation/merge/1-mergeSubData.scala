import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.hadoop.fs.{FileSystem, FileStatus, Path}
import org.apache.hadoop.conf.Configuration

object MergeParquetFiles {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("MergeParquetFiles")
      .getOrCreate()

    val hdfsFolder = "hdfs://nyu-dataproc-m/user/tw2883_nyu_edu/bdad_project/downsampSubElevationDataFrame/"

    // Create a Hadoop configuration
    val hadoopConf = new Configuration()

    // Get the Hadoop FileSystem
    val hdfs = FileSystem.get(hadoopConf)

    // Retrieve a list of FileStatus objects from HDFS
    val files: Array[FileStatus] = hdfs.listStatus(new Path(hdfsFolder))

    // Read Parquet files and union them into a single DataFrame
    var masterDF: DataFrame = null

    for (fileStatus <- files) {
      val parquetPath = fileStatus.getPath.toString

      // Read Parquet file
      val parquetDF = spark.read.parquet(parquetPath)

      // Union with the master DataFrame
      if (masterDF == null) {
        masterDF = parquetDF
      } else {
        masterDF = masterDF.union(parquetDF)
      }
    }

    // Save the master DataFrame to HDFS
    val outputFolder = "hdfs://nyu-dataproc-m/user/tw2883_nyu_edu/bdad_project/masterElevation/"
    masterDF.write.mode("overwrite").parquet(outputFolder)

    spark.stop()
  }
}