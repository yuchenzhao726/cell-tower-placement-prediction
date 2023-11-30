import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, udf}
import geotrellis.vector.{Polygon, Point}
import geotrellis.proj4._


object BuildingFootprint {
  def main(args: Array[String]): Unit = {
    val TRAIN_LONGITUDE_L = -125.0
    val TRAIN_LONGITUDE_H = -71.0
    val TRAIN_LATITUDE_L = 33.0
    val TRAIN_LATITUDE_H = 42.0

    val TEST_LONGITUDE_L = -124.0
    val TEST_LONGITUDE_H = -117.0
    val TEST_LATITUDE_L = 46.0
    val TEST_LATITUDE_H = 48.0

    if (args.length < 3) {
      System.err.println("Usage: BuildingFootprint <input-directory> <train-output> <test-output>")
      System.exit(1)
    }
    // Create the Spark session
    val spark = SparkSession.builder.appName("BuildingFootprint").getOrCreate()

    //Read the geoJson file from the directory
    val df = spark.read.option("multiLine", true).json(args(0))
    //Expand the polygon coordinates under tag features
    val buildingDF = df.select(explode(col("features")).alias("building"))
    //Create a flat structured DataFrame
    val flatBuildingDF = buildingDF.select(
      col ("building.geometry.coordinates").getItem(0).alias("coordinates")
    )

    //Define the UDF to get polygon centroid
    val getCentroid = udf((points: Array[Array[Double]]) => {
      val polygon: Polygon = createPolygon(points)
      val center: Point = getPolygonCenter(polygon)
      (center.x, center.y)
    })
    //Apply getPolygonCentroid UDF to the coordinates column and create two new column for the centroid
    val resultDF = flatBuildingDF.withColumn("polygon_centroid", getCentroid(col("coordinates")))
      .withColumn("center_longitude", col("polygon_centroid._1"))
      .withColumn("center_latitude", col("polygon_centroid._2"))
      .drop("polygon_centroid")

    //Filter the result to get the training and test test set
    val trainDF = resultDF.filter(
      col("center_longitude") <= TRAIN_LONGITUDE_H && 
      col("center_longitude") >= TRAIN_LONGITUDE_L &&
      col("center_latitude") <= TRAIN_LATITUDE_H &&
      col("center_latitude") >= TRAIN_LATITUDE_L)
    val testDF = resultDF.filter(
      col("center_longitude") <= TEST_LONGITUDE_H && 
      col("center_longitude") >= TEST_LONGITUDE_L &&
      col("center_latitude") <= TEST_LATITUDE_H &&
      col("center_latitude") >= TEST_LATITUDE_L)


    //Define the UDF to get polygon area
    val getArea = udf((points: Array[Array[Double]]) => {
      val polygon: Polygon = createPolygon(points)
      val area: Double = getPolygonArea(polygon)
      area
    })
    //Apply getPolygonInfo UDF to the coordinates column and add new columns as area
    val resultTrainDF = trainDF.withColumn("area", getArea(col("coordinates")))
      .drop("coordinates")
    val resultTestDF = testDF.withColumn("area", getArea(col("coordinates")))
      .drop("coordinates")

    resultTrainDF.write.mode("append").format("parquet").option("compression", "snappy").save(args(1))
    resultTestDF.write.mode("append").format("parquet").option("compression", "snappy").save(args(2))

    spark.stop()
  }

  def createPolygon(points: Array[Array[Double]]): Polygon = {
    val coordinates = points.map { case Array(x, y) => Point(x, y) }
    Polygon(coordinates :+ coordinates.head)
  }

  def getPolygonCenter(polygon: Polygon): Point = {
    val centroid = polygon.centroid.as[Point]
    centroid.getOrElse(polygon.vertices(0))
  }

  def getPolygonArea(polygon: Polygon): Double = {
    val targetCRS = CRS.fromEpsgCode(3857)
    val projectedPolygon = polygon.reproject(LatLng, targetCRS)
    projectedPolygon.area
  }
}
