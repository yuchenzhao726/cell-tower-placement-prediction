name := "MergeParquetFiles"

version := "1.0"

scalaVersion := "2.12.14"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.apache.hadoop" % "hadoop-common" % "3.3.0",
  "org.apache.hadoop" % "hadoop-client" % "3.3.0",
  "org.apache.hadoop" % "hadoop-hdfs" % "3.3.0"
)
