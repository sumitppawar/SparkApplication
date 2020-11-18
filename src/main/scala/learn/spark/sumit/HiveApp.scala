package learn.spark.sumit

import org.apache.spark.sql.SparkSession

object HiveApp {
  def main(args: Array[String]): Unit = {
    val spark =   SparkSession
    .builder
      .master("local[*]")
    .appName("Python Spark SQL Hive integration example")
    .config("spark.sql.uris", "thrift://127.0.0.1:9083")
    .enableHiveSupport()
    .getOrCreate()
    spark.sql("show databases").show()
  }
}


/*
Given an array of integers, shift all zeros to the end of an array by keeping the order of non-zero.
input  =>  [1,0,3,4,0,6,7,8]
output =>  [1,3,4,6,7,8,0,0]
 */