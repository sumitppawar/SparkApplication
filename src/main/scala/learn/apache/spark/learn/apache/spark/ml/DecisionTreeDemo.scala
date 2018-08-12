package learn.apache.spark.learn.apache.spark.ml

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Row}
import spark.sqlContext.implicits._

object DecisionTreeDemo {
  def main(args: Array[String]): Unit = {
    val dataSet = spark
      .read
      .format("csv")
      .option("header", "false")
      .load("/Users/sumit/Downloads/datasets/wine.data")


    val vectorisedData =  vectorise(dataSet)

    val labelIndexer = new StringIndexer()
      .setInputCol("label")
        .setOutputCol("indexedLabel")
    val indexedData = labelIndexer.fit(vectorisedData).transform(vectorisedData)

    val splitData = indexedData.randomSplit(Array(0.8,0.2))
    val (trainingData, testData) = (splitData(0), splitData(1))

    val dtree = new DecisionTreeClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("features")
        .setMaxDepth(3)
        .setImpurity("gini")
    val model = dtree.fit(indexedData)

    val transformedData = model.transform(testData)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("f1")

    println(s"accuracy: ${evaluator.evaluate(transformedData)}")

  }

  def vectorise(dataSet: DataFrame): DataFrame =
    dataSet.rdd.map{ row =>
      val seq = row.toSeq.map(_.asInstanceOf[String].toDouble)
      (seq(0), Vectors.dense(seq.tail.toArray))
    }.toDF("label","features")

}
