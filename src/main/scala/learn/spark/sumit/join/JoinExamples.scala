package learn.spark.sumit.join

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object JoinExamples {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Spark join two df")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val transactionDf = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("/Users/pawarsumit/Downloads/TRANSACTIONS_TRAIN_20200812101904.csv")
      .withColumnRenamed("PRIMARY_PARTY_KEY", "PARTY_KEY")

    val customer = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("/Users/pawarsumit/Downloads/CUSTOMERS_TRAIN_20200812101904.csv")
    //transactionDf.printSchema()
    //customer.printSchema()

/*    transactionDf
      .groupBy(col("PARTY_KEY"))
      .agg(count("PARTY_KEY").as("count"))
      .orderBy(desc("count"))
      .show()*/

    val df = transactionDf.as("df1")
      .join(broadcast(customer.as("df2")), col("df1.PARTY_KEY") === col("df2.PARTY_KEY"))
      .groupBy(col("df1.PARTY_KEY"))
      .agg(count("df1.PARTY_KEY").as("count"))
      .orderBy(desc("count"))

    df.explain()
    df.collect()
      .foreach(println)


    while(true) {}

  }
}
