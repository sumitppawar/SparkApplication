package learn.spark.sumit.join

import org.apache.spark.sql.SparkSession

object SortMergeJoin {
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
    //Default is sort-merge join
    //spark.sql.joins.preferSortMergeJoin (default true)

  }
}
