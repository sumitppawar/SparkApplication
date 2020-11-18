package learn.spark.sumit.s3

import org.apache.spark.sql._

object SparkWithS3 {
  def main(args: Array[String]): Unit = {
    val spark =
       SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark with S3")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIATMLDM7P4HIY6BKKQ")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "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")

    val df = spark
      .read
      .text("/Users/pawarsumit/workspace/SparkApplication/src/main/scala/learn/spark/sumit/s3/SparkWithS3.scala")

    df.show()

    df
      .write
      .text("s3a://com.tookitaki.learns/sumit/SparkWithS3.scala")
  }
}


