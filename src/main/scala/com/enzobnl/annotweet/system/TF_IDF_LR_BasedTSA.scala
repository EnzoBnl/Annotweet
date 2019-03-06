package com.enzobnl.annotweet.system

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature._
import com.enzobnl.annotweet.system.Tag
import com.enzobnl.annotweet.utils.Utils
import org.apache.spark.sql.DataFrame
class TF_IDF_LR_BasedTSA extends TweetSentimentAnalyzer {
  override def tag(tweet: String): Tag.Value = {
    Tag.get(_pipelineModel.transform(
      _spark.createDataFrame(Seq(Tuple3("0", "???", tweet)))
      .toDF("id", "target", "text"))
      .select("unindexedLabel").collect()(0).getAs[String]("unindexedLabel"))
  }
  override def train(trainDF: DataFrame, params: Map[String, AnyVal]): Double = {
    Utils.getTime {
      val maxIter: Int= params.getOrElse("maxIter", 100).asInstanceOf[Int]
      val numFeatures: Int = params.getOrElse("numFeatures", Math.pow(2, 16).toInt).asInstanceOf[Int]
      val minDocFreq: Int = params.getOrElse("minDocFreq", 5).asInstanceOf[Int]
      println(maxIter, numFeatures, minDocFreq)
      val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
      val hashtf = new HashingTF().setNumFeatures(numFeatures).setInputCol("words").setOutputCol("tf")
      val idf = new IDF().setMinDocFreq(minDocFreq).setInputCol("tf").setOutputCol("features")
      //minDocFreq: remove sparse terms
      val targetToIndexedLabel = new StringIndexer().setHandleInvalid("keep").setInputCol("target").setOutputCol("label")
      val labels = new StringIndexer().setHandleInvalid("keep").setInputCol("target").setOutputCol("label").fit(trainDF).labels
      val lr = new LogisticRegression().setMaxIter(maxIter).setFeaturesCol("features").setLabelCol("label").setPredictionCol("predictedLabel")
      val indexedLabelToTarget = new IndexToString().setInputCol("predictedLabel").setOutputCol("unindexedLabel").setLabels(labels)
      val pipeline = new Pipeline().setStages(Array(tokenizer, hashtf, idf, targetToIndexedLabel, lr, indexedLabelToTarget))
      this._pipelineModel = pipeline.fit(trainDF)
    }
  }
}