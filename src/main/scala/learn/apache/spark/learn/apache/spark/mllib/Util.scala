package learn.apache.spark.learn.apache.spark.mllib

import org.apache.spark.{SparkConf, SparkContext}

object Util {

  private val config = new SparkConf().setMaster("local").setAppName("SparkApplication")
  lazy  val sc: SparkContext = new SparkContext(config)

}
