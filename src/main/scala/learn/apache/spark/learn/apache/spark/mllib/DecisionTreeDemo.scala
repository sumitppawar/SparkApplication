package learn.apache.spark.learn.apache.spark.mllib

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import Util._

object DecisionTreeDemo {
  def main(args: Array[String]): Unit = {

    val rawData = sc.textFile("docs/wine.data")
    val mappedData = rawData.map(record => record.split(",").map(_.toDouble) )
    val labeledData = mappedData.map(record => LabeledPoint(record.head, toFeatureVector(record.tail)))
    val split = labeledData.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (split(0), split(1))

    //Tree
    val model = DecisionTree.trainClassifier(
      input = trainingData,
      numClasses = 4,
      categoricalFeaturesInfo = Map.empty[Int, Int],
      impurity = "gini",
      maxDepth = 3,
      maxBins = 32
    )

    val predictions = testData.map(lbl => model.predict(lbl.features))
    val predictionsAndLabel = predictions zip testData.map(_.label)

    //Accuracy
    val manulaAccuracy = predictionsAndLabel.filter{case(p, a) => p == a}.count / testData.count.toDouble
    println(s"Manual accuracy: $manulaAccuracy" )

    //In-built Matrix classes
    val matrix = new MulticlassMetrics(predictionsAndLabel)
    println(s"MulticlassMetrics accuracy: ${matrix.accuracy}")

  }

  def toFeatureVector(features: Array[Double]): org.apache.spark.mllib.linalg.Vector =
    org.apache.spark.mllib.linalg.Vectors.dense(features)
}
