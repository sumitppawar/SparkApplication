package learn.spark.sumit
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
object Windowing {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Data Windowing")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    val window = Window
      .partitionBy($"country")
      .orderBy(s"age")
      .rowsBetween(-4,0)
    val agger = avg($"age").over(window)

    val df = spark.read.option("header", "true").csv("/Users/pawarsumit/software/spark-2.4.5-bin-hadoop2.7/data/users.csv")

    df
      .withColumn("wid", agger)
      .show()
  }

}
