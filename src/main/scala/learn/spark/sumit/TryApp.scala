package learn.spark.sumit

import org.apache.spark.sql.SparkSession

object TryApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Awesome")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val textFile = spark.read.format("text").load("/Users/pawarsumit/Desktop/3.txt").as[String]
    val counts =
      textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .groupBy("_1")
      .count()

    counts.show(10)

    val textFile2 = spark.read.format("text").load("/Users/pawarsumit/Desktop/3.txt")
    textFile2
      .groupBy()

    spark.sparkContext

  }
}
