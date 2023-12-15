import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, DoubleType, StringType}
import org.apache.spark.sql.DataFrame
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, FileStatus, Path}
import org.apache.hadoop.conf.Configuration
import geotrellis.spark.store.hadoop.HadoopGeoTiffRDD
import geotrellis.spark.io.hadoop._
import geotrellis.vector.Extent
import geotrellis.raster.MultibandTile
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.io.geotiff._
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster._

object ElevationProcessing {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("ElevationProcessing")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext

    val elevationFolder = "hdfs://nyu-dataproc-m/user/tw2883_nyu_edu/bdad_project/elevation_1arc/"
    val outputFolder = "hdfs://nyu-dataproc-m/user/tw2883_nyu_edu/bdad_project/downsampSubElevationDataFrame/"

    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)

    // Retrieve a list of FileStatus objects from HDFS
    val tiffFiles: Array[FileStatus] = hdfs.listStatus(new Path(elevationFolder))

    // Define a UDF to process "single" tiffFile
    def processGeoTiff(tiffFile: String): Unit = {
      // Load GeoTiff file
      val tiffRDD: RDD[(Extent, MultibandTile)] = sc.binaryFiles(tiffFile).map { case (_, content) =>
        val stream = content.open()  // Open the PortableDataStream
        try {
          val bytes = org.apache.commons.io.IOUtils.toByteArray(stream)  // Convert PortableDataStream to byte array
          val geoTiff = MultibandGeoTiff(bytes)
          val extent = geoTiff.extent
          (extent, geoTiff.tile)
        } finally {
          stream.close()  // Close the PortableDataStream
        }
      }

      // Convert to DataFrame
      val parsedDataFrame = tiffRDD.flatMap { case (extent, multibandTile) =>
        val lat = extent.ymax
        val lon = extent.xmin
        val cols = multibandTile.cols
        val rows = multibandTile.rows

        val elevationArray = multibandTile.bands.flatMap(_.toArrayDouble())

        // Downsampling here - keep nearly 0.01 degree - around 1 km (granularity)
        val downsampledRows = (0 until rows by 100).flatMap { row =>
          (0 until cols by 100).map { col =>
            val elevation = elevationArray(row * cols + col)
            val longitude = lon + col.toDouble / cols
            val latitude = lat - row.toDouble / rows * extent.height
            (longitude, latitude, elevation)
          }
        }

        downsampledRows
      }.toDF("longitude", "latitude", "elevation")

      // Replace NaN with null
      val newParsedDataFrame = parsedDataFrame.withColumn("elevation", when(isnan($"elevation"), lit(null)).otherwise($"elevation"))

      // Drop rows containing null in the "elevation" column
      val finalDataFrame = newParsedDataFrame.na.drop("all", Seq("elevation"))

      // Save DataFrame to HDFS
      val fileName = new Path(tiffFile).getName.replaceAll("\\.tif", "")
      val outputPath = s"$outputFolder/$fileName"

      finalDataFrame.write.mode(SaveMode.Overwrite).parquet(outputPath)
    }

    // Implement a for-loop to process each tiffFile
    for (fileStatus <- tiffFiles) {
      processGeoTiff(fileStatus.getPath.toString)
    }

    spark.stop()
  }
}