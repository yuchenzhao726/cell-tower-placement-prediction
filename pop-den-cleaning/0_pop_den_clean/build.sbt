name := "PopDenCleanApp"
version := "1.0"
scalaVersion := "2.12.14"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.locationtech.geotrellis" %% "geotrellis-spark" % "3.1.0"
)
