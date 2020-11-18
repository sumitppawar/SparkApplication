package learn.spark.sumit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object KafkaConnector {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Kafka connect")
      .master("local[*]")
      .config("spark.streaming.receiver.writeAheadLog.enable", true)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val  df  = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingoffsets", "latest")
      .option("subscribe", "delta-alerts")
      .load()


    df
      .selectExpr("CAST(partition as INT)", "CAST(offset as INT)")
      .withWatermark("timestamp", "2 seconds")
      .groupBy("partition")
      .agg(max("offset"))
      .writeStream
      .format("console")
      .start()



    import spark.implicits._
     df
      .selectExpr(s"CAST(value as STRING)", "CAST(partition as INT)", "CAST(offset as INT)")
      .as[DeltaAlertsKafkaMessage]
      .writeStream
      .outputMode("append")
      .format("csv")
      .option("path","/Users/pawarsumit/workspace/SparkApplication/out")
      .option("checkpointLocation", "/Users/pawarsumit/workspace/SparkApplication/checkpnt")
      .start()
      .awaitTermination()



  }
}

case class DeltaAlertsKafkaMessage(
                                    value: String,
                                    partition: Int,
                                    offset: Int
                                  )

