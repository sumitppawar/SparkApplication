package learn.spark.sumit
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

object FileStreamReader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("File stream")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
   val schema = new StructType()
     .add("COLUMN", "STRING")
     .add("DATA_TYPE", "STRING")

    val df = spark
      .readStream
      .option("header", "true")
      .schema(schema)
      .csv("/Users/pawarsumit/workspace/SparkApplication/data2")

    val resultDf = df
      .select(
        col("COLUMN").as("column"),
        col("DATA_TYPE").as("data_type")
      )


    val query = resultDf
      .writeStream
      .format("console")
      .start()

    query.awaitTermination()
  }
}


