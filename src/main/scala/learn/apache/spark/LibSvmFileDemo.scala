package learn.apache.spark

import org.apache.spark.mllib.util.MLUtils
import Util._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.tree.DecisionTree

object LibSvmFileDemo {
  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")
    val labeledData = MLUtils.loadLibSVMFile(sc, "docs/wine.scale")
    val split = labeledData.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (split(0), split(1))

    val model = DecisionTree.trainClassifier(
      trainingData,
      4,
      Map.empty[Int, Int],
      "gini",
      5,
      32
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
}
