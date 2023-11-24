import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, udf}
import geotrellis.vector.{Polygon, Point}


object BuildingFootprint {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: BuildingFootprint <input-directory> <output-directory>")
      System.exit(1)
    }
    // Create the Spark session
    val spark = SparkSession.builder.appName("BuildingFootprint").getOrCreate()

    //Read the geoJson file from the directory
    val df = spark.read.option("multiLine", true).json(args(0))
    //Expand the polygon coordinates under tag features
    val buildingDF = df.select(explode(col("features")).alias("building"))
    //Create a flat structured DataFrame
    val flatDF = buildingDF.select(
      col ("building.geometry.coordinates").getItem(0).alias("coordinates"),
      col ("building.geometry.type").alias("type")
    )

    //Define the UDF to get polygon center and area
    val getPolygonInfo = udf((points: Array[Array[Double]]) => {
      val polygon: Polygon = createPolygon(points)
      val center: Point = getPolygonCenter(polygon)
      val area: Double = getPolygonArea(polygon)
      (center.x, center.y, area)
    })
    //Apply getPolygonInfo UDF to the coordinates column and add new columns as center and area
    val resultDF = flatDF.withColumn("polygon_info", getPolygonInfo(col("coordinates")))
      .withColumn("center_latitude", col("polygon_info._1"))
      .withColumn("center_longitude", col("polygon_info._2"))
      .withColumn("area", col("polygon_info._3"))
      .drop("polygon_info")

    resultDF.write.mode("overwrite").format("parquet").option("compression", "snappy").save(args(1))

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
    polygon.area
  }
}
