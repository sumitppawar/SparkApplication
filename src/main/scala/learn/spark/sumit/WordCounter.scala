package learn.spark.sumit

import org.apache.spark.sql.SparkSession

object WordCounter  {
   def main(args: Array[String]): Unit = {

/*     val conf = new SparkConf().setAppName("WordCounter")
     val sc = new SparkContext(conf)*/

    args.foreach(arg => println(s"User arguments $arg"))
    val spark = SparkSession
      .builder()
      .appName("WordCounter")
      .getOrCreate()

     import spark.implicits._
     val  words = spark
       .read
       .textFile("/Users/pawarsumit/workspace/SparkApplication/src/main/scala/learn/spark/sumit/WordCounter.scala")
       .flatMap(lines => lines.split(" "))
      .map(word => (word, 1))
      .groupByKey(_._1)
      .count()
     words.write.format("csv").save("/Users/pawarsumit/workspace/SparkApplication/src/main/scala/learn/spark/sumit/abc.txt")
     spark.stop()
  }
}
