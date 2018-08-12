package learn.apache.spark.learn.apache.spark

import org.apache.spark.sql.SparkSession

package object ml {
 val spark = SparkSession
   .builder
     .master("local")
   .appName("SparkApp")
   .getOrCreate()

}
