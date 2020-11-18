package learn.spark.sumit

import org.apache.spark.sql.{SaveMode, SparkSession}

object DataFrameJson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Json Reader")
      .config("spark.driver.memory", "3g")
      .getOrCreate()

    import spark.implicits._
    val jsonDf = spark
      .read
      .json("/Users/pawarsumit/workspace/SparkApplication/data/frinds.json")
      .na.drop()
      .na.replace("name", Map("Inez Schroeder" -> "Sumit Pawar"))
      .na.fill("female", Seq("gender"))
      .where($"name" === "Sumit Pawar")

    jsonDf.checkpoint()
    jsonDf.show()
    jsonDf.count()
    jsonDf.printSchema()
    jsonDf
      .write.mode(SaveMode.Overwrite)
      .json("/Users/pawarsumit/workspace/SparkApplication/data/out/json")

    spark.stop()

  }
}
