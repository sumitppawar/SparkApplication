
name := "SparkApplication"
version := "0.1"
scalaVersion := "2.11.11"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5" ,
  "org.apache.spark" %% "spark-mllib" % "2.4.5" ,
  "org.apache.spark" %% "spark-sql" % "2.4.5" ,
  "org.apache.spark" %% "spark-hive" % "2.4.5" ,
  "org.apache.spark" %% "spark-streaming" % "2.4.5" ,
  "org.apache.spark" %% "spark-graphx" % "2.4.5",
  //"com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3",
  //"com.amazonaws" % "aws-java-sdk" % "1.11.199",
  //"org.apache.hadoop" % "hadoop-common" % "3.2.1",
 //"org.apache.hadoop" % "hadoop-aws" % "3.2.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.7",
  "com.crealytics" % "spark-excel_2.11" % "0.12.2",
  "org.json4s" %% "json4s-native"  % "3.5.2", // apache
  "org.json4s" %% "json4s-jackson" % "3.5.2" //apache
)