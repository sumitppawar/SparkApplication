package learn.spark.sumit
import java.sql.Timestamp

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SocketStreamWindowExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Socket stream window count")
      .getOrCreate()

    import spark.implicits._
    val df = spark
      .readStream
      .option("port", 9999)
      .option("host", "localhost")
      .format("socket")
      .load()

    df.printSchema()
    spark.sparkContext.setLogLevel("ERROR")
    val signaledCount =  df
      .as[String]
      .map(line => (line.split(",")(0), line.split(",")(1)))
      .select(col("_1").as("device_id"), col("_2").as("timestamp").cast("TIMESTAMP"))
      .groupBy(
      window(col("timestamp"), "10 minutes", "5 minutes"),
      col("device_id")
    ).count()

    signaledCount.writeStream.outputMode("update").format("console").start().awaitTermination()

  }
}

case class Data(
                 device_id: String,
                 timestamp: Timestamp
               )