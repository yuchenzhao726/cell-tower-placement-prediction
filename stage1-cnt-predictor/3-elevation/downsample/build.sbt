name := "ElevationProcessing"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "org.apache.hadoop" % "hadoop-common" % "3.3.0",
  "org.apache.hadoop" % "hadoop-client" % "3.3.0",
  "org.apache.hadoop" % "hadoop-hdfs" % "3.3.0",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.0",
  "org.apache.commons" % "commons-io" % "1.3.2",
  "org.locationtech.geotrellis" %% "geotrellis-raster" % "3.5.0",
  "org.locationtech.geotrellis" %% "geotrellis-vector" % "3.5.0",
  "org.locationtech.geotrellis" %% "geotrellis-store" % "3.5.0"
)


mainClass in Compile := Some("ElevationProcessing")

sbtVersion := "1.5.5"
