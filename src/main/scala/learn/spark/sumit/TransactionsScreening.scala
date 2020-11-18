package learn.spark.sumit

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, _}

object TransactionsScreening {
  def main(args: Array[String]): Unit = {
    //use nc -lk 9999
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("TransactionsScreening")

    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 9999)
    lines
      .flatMap(_.split(" "))
        .map(word => (word, 1))
        .reduceByKey(_ + _)
        .print()


    ssc.start()
    ssc.awaitTermination()

  }
}
