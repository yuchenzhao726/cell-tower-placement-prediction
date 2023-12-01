name := "ElevationProcessing"

version := "1.0"

scalaVersion := "2.12.14"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.apache.hadoop" % "hadoop-common" % "3.3.0",
  "org.apache.hadoop" % "hadoop-client" % "3.3.0",
  "org.apache.hadoop" % "hadoop-hdfs" % "3.3.0",
  "org.datasyslab" % "geotrellis-raster_2.12" % "3.7.1",
  "org.datasyslab" % "geotrellis-vector_2.12" % "3.7.1"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("org.apache.commons.io.**" -> "shadedio.@1").inAll
)
