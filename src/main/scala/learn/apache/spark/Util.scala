package learn.apache.spark

import learn.apache.spark.DriverProgram.createSparkConf
import org.apache.spark.{SparkConf, SparkContext}

object Util {

  private val config = new SparkConf().setMaster("local").setAppName("SparkApplication")
  lazy  val sc: SparkContext = new SparkContext(config)

}
