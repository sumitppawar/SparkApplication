package learn.apache.spark.learn.apache.spark.ml

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._


object SpamDetection {
  def main(args: Array[String]): Unit = {
    import spark.sqlContext.implicits._

    val remover = new StopWordsRemover()
      .setInputCol("words")
      .setOutputCol("filtered")

    val hashingTF = new HashingTF()
      .setInputCol("filtered").setOutputCol("features").setNumFeatures(20)
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val ngram = new NGram().setN(2).setInputCol("words").setOutputCol("filtered")


    val spamData = spark
      .read
      .format("csv")
      .load("/Users/sumit/Workspace/SparkApplication/docs/spam.txt")

    val sentenceSpamData = spamData
      .rdd
      .map(row => (0.0, row.getAs[String](0)))
      .toDF("label", "sentence")



    val spamWordsData = tokenizer.transform(sentenceSpamData.na.drop(Array("sentence")))
    val spamStopWord = remover.transform(spamWordsData)
    val featurizedSpamData = hashingTF.transform(spamStopWord)



    val hamData = spark
      .read
      .format("csv")
      .load("/Users/sumit/Workspace/SparkApplication/docs/ham.txt")

    val sentenceHamData = hamData
      .rdd
      .map(row => (1.0, row.getAs[String](0)))
      .toDF("label", "sentence")

    val hamWordsData = tokenizer.transform(sentenceHamData.na.drop(Array("sentence")))
    val hamStopWord = remover.transform(hamWordsData)
    val featurizedHamData = hashingTF.transform(hamStopWord)




    val data = featurizedSpamData
      .union (featurizedHamData)

    val spamNgrams = ngram.transform(spamWordsData)
    val featurizedSpamNgramData = hashingTF.transform(spamNgrams)
    val HamNgrams = ngram.transform(hamWordsData)
    val featurizedHamNgramData = hashingTF.transform(HamNgrams)

    val dataWithGrams = featurizedSpamNgramData union featurizedHamNgramData

    val finalData = data union dataWithGrams

    val idf = new IDF().setInputCol("features").setOutputCol("finalFeatures")
    val idfModel = idf.fit(finalData)

    val rescaledData = idfModel.transform(finalData)
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")

    val indexedData = labelIndexer.fit(rescaledData).transform(rescaledData)


    val splitData = indexedData.randomSplit(Array(0.8,0.2))
    val (trainingData, testData) = (splitData(0), splitData(1))

    val dtree = new DecisionTreeClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("finalFeatures")
      .setMaxDepth(30)
      .setImpurity("gini")
    val model = dtree.fit(indexedData)

    val transformedData = model.transform(testData)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("f1")

    println(s"accuracy: ${evaluator.evaluate(transformedData)}")


    import spark.implicits._
    val userTestSen = spark
      .sqlContext
      .sparkContext
      .parallelize(List("Dad please send me some money for this month"))
      .toDF("sentence")

    val userWordsData = tokenizer.transform(userTestSen.na.drop(Array("sentence")))
    val userStopWord = remover.transform(userWordsData)
    val userNgrams = ngram.transform(userWordsData)
    val userFinalData = userStopWord union userNgrams
    val userHT = hashingTF.transform(userFinalData)
    val userRescaledData = idf.fit(userHT).transform(userHT)

    val predication = model.transform(userRescaledData)
    predication.show()

  }

}
