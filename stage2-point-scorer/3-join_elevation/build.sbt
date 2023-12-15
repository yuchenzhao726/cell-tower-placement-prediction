name := "JoinData"

version := "1.0"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8"
)

mainClass in Compile := Some("JoinData")

sbtVersion := "1.5.5"
