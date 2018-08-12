package learn.apache.spark.learn.apache.spark.mllib

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

object DriverProgram {
  def main(args: Array[String]): Unit = {

    val sc = createSparkContext()
    //Load Data
    val spamRdd = sc.textFile("docs/spam.txt")
    val nonSpamRdd = sc.textFile("docs/nonspam.txt")

    val tf = new HashingTF()
    //Feature Extraction
    val spamFeature = spamRdd.map(line => tf.transform(line.toUpperCase.split(" ")))
    val nonSpamFeature = nonSpamRdd.map(line => tf.transform(line.toUpperCase.split(" ")))

    //Label Data
    val negativeExamples = spamFeature.map(feature => LabeledPoint(1, feature))
    val positiveExamples = nonSpamFeature.map(feature => LabeledPoint(0, feature))

    //Model
    val trainingData = positiveExamples union negativeExamples
    trainingData.cache()
    val model = new LogisticRegressionWithLBFGS().run(trainingData)

    val negativeTest = tf.transform(
      "Additional income Click here link Free cell phone"
        .toUpperCase
        .split(" ")
    )

    val positveTest = tf.transform(
      "Hey Dad, How are you ? Plaese send some extra money for this month"
        .toUpperCase
        .split(" ")
    )

    println(s"Predication for positive test example: ${model.predict(positveTest)}")
    println(s"Predication for negative test example: ${model.predict(negativeTest)}")
  }

  private def createSparkContext():SparkContext = new SparkContext(createSparkConf())
  private def createSparkConf(): SparkConf = {
    new SparkConf()
      .setMaster("local")
      .setAppName("SparkApplication")
  }

}
