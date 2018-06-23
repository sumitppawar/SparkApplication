name := "SparkApplication"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.3.1",
  "com.typesafe.play" %% "play-json" % "2.6.7"
)