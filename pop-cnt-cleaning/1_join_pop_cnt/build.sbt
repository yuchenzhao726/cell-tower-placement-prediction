name := "JoinPopCntApp"
version := "1.0"
scalaVersion := "2.12.14"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.apache.sedona" %% "sedona-spark-shaded-3.0" % "1.5.0",
  "org.datasyslab" % "geotools-wrapper" % "1.5.0-28.2"
)
