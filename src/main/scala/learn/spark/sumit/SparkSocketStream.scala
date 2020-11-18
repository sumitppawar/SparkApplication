package learn.spark.sumit
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkSocketStream {

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder
        .master("local[*]")
        .appName("Spark socket stream")
        .config("spark.streaming.receiver.writeAheadLog.enable", true)
        .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")

    val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()



    import spark.implicits._
    val c = lines
      .flatMap(_.getString(0).split(" "))
      .groupBy("value")
      .count()

      val s = c.writeStream.outputMode("update")

      .format("console")
      .start()
    s.awaitTermination()

  }

}
