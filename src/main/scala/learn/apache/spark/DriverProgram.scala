package learn.apache.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.HashingTF

object DriverProgram {
  def main(args: Array[String]): Unit = {
    val sc = createSparkContext()
    sc.textFile("build.sbt")
    print(sc.textFile("build.sbt").count)
    sc.stop
  }

  private def createSparkContext():SparkContext = new SparkContext(createSparkConf())
  private def createSparkConf(): SparkConf = {
    new SparkConf()
      .setMaster("local")
      .setAppName("SparkApplication")
  }

}
