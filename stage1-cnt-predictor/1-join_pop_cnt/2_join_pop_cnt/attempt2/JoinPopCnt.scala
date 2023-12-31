// sbt package
// spark-submit --packages org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.5.0,org.datasyslab:geotools-wrapper:1.5.0-28.2 --conf spark.yarn.maxAppAttempts=2 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --class JoinPopCnt target/scala-2.12/joinpopcntapp_2.12-1.0.jar 
// nohup spark-submit --packages org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.5.0,org.datasyslab:geotools-wrapper:1.5.0-28.2 --conf spark.yarn.maxAppAttempts=2 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --class JoinPopCnt target/scala-2.12/joinpopcntapp_2.12-1.0.jar > run.log 2>&1 &
// yarn application -list -appStates RUNNING
// yarn application -kill application_1691775874963_33065

import org.apache.spark.sql._
import org.apache.spark.sql.Encoders
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.locationtech.jts.geom.{GeometryFactory, Coordinate, Point}
import org.locationtech.jts.io.WKTWriter
import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.Adapter
import org.apache.sedona.core.enums.{GridType, IndexType}
// import org.apache.sedona.core.spatialOperator.JoinQuery

object JoinPopCnt {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                                .appName("JoinPopCnt")
                                .getOrCreate()
        
        import spark.implicits._

        val popInputDir = "/user/gw2310_nyu_edu/bdad_proj/pop_cnt_clean" 
        val towerInputDir = "/user/gw2310_nyu_edu/bdad_proj/training_set_1" 
        val outputDir = "/user/gw2310_nyu_edu/bdad_proj/training_set_2"

	    SedonaSQLRegistrator.registerAll(spark)

	    val dfPoints = spark.read.parquet(popInputDir)

	    val dfRectangles = spark.read.parquet(towerInputDir)

	    // build java polygon object for random generated rectangle by its points
	    val dfRectanglesWithGeom = dfRectangles.map(row => {
	        val coordinates = Array(
		        new Coordinate(row.getDouble(0), row.getDouble(1)),
		        new Coordinate(row.getDouble(2), row.getDouble(1)),
		        new Coordinate(row.getDouble(2), row.getDouble(3)),
		        new Coordinate(row.getDouble(0), row.getDouble(3)),
		        new Coordinate(row.getDouble(0), row.getDouble(1))
	        ) 
	        val geometryFactory = new GeometryFactory()
	        val polygon = new GeometryFactory().createPolygon(coordinates)
			// serialization tools
	        val wktWriter = new WKTWriter()
	        (wktWriter.write(polygon), row.getDouble(4), row.getLong(5))
	    }).toDF("geometry", "area", "cell_tower_num")
	    dfRectanglesWithGeom.createOrReplaceTempView("rectanglesWithGeom")
		// just use a stupid method to get a dataframe whose geometry is a java object not a serialized string、
		// ST_GeomFromWKT is for deserialization
	    val rectangles = spark.sql("SELECT ST_GeomFromWKT(geometry) AS geometry, area, cell_tower_num FROM rectanglesWithGeom")

	    // build point object
	    val dfPointsWithGeom = dfPoints.map(row => {
	        val geometryFactory = new GeometryFactory()
	        val wktWriter = new WKTWriter()
	        val point = geometryFactory.createPoint(new Coordinate(row.getDouble(0), row.getDouble(1)))
	        (wktWriter.write(point), row.getDouble(2))
	    }).toDF("geometry", "pop_cnt")
	    dfPointsWithGeom.createOrReplaceTempView("pointsWithGeom")
		// the same way as polygon object
	    val points = spark.sql("SELECT ST_GeomFromWKT(geometry) AS geometry, pop_cnt FROM pointsWithGeom")

        // dataframe has to be transformed to RDD for knowing spatial boundary
        // otherwise, there is null point exception when doing SQL queries in the final step
        val polygonRDD = Adapter.toSpatialRdd(rectangles, "geometry")
        val pointRDD = Adapter.toSpatialRdd(points, "geometry")

        // learn about boundaries
        polygonRDD.analyze()
		// build spatail partition
        polygonRDD.spatialPartitioning(GridType.KDBTREE)
		// make the partition of points the same as polygons
        pointRDD.spatialPartitioning(polygonRDD.getPartitioner)
		// build spatial index
        polygonRDD.buildIndex(IndexType.RTREE, true)

		// convert back to dataframe 
        val polygonDF = Adapter.toDf(polygonRDD, spark)
        val pointDF = Adapter.toDf(pointRDD, spark)

        polygonDF.createOrReplaceTempView("rectangles")
        pointDF.createOrReplaceTempView("points")

		// ST_AsText for serialization
		// WHERE ST_Contains(r.geometry, p.geometry) for an inner join between points and the regions containing these points.
        val joinedDF = spark.sql("""
                        SELECT ST_AsText(r.geometry) as rectangle, r.area, r.cell_tower_num, SUM(p.pop_cnt) as pop_cnt_sum
                        FROM rectangles r, points p
                        WHERE ST_Contains(r.geometry, p.geometry)
                        GROUP BY r.geometry, r.area, r.cell_tower_num
                    """)

        joinedDF.write.mode("overwrite").parquet(outputDir)

        spark.stop()
    }
}

