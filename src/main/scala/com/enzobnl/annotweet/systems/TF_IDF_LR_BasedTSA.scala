package com.enzobnl.annotweet.systems

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature._
import com.enzobnl.annotweet.utils.Utils
import org.apache.spark.sql.DataFrame
import org.glassfish.jersey.process.internal.Stages
class TF_IDF_LR_BasedTSA extends TweetSentimentAnalyzer {
  override def tag(tweet: String): Tag.Value = {
    println(_pipelineModel.transform(
      _spark.createDataFrame(Seq(Tuple3("0", "???", tweet)))
        .toDF("id", "target", "text")).collect()(0).getAs[String]("words"))
    Tag.get(_pipelineModel.transform(
      _spark.createDataFrame(Seq(Tuple3("0", "???", tweet)))
      .toDF("id", "target", "text"))
      .select("unindexedLabel").collect()(0).getAs[String]("unindexedLabel"))
  }
  override def train(trainDF: DataFrame=_df, params: Map[String, AnyVal]=Map(), stages: Array[PipelineStage]=Array()): Double = {
    val maxIter: Int= params.getOrElse("maxIter", 100).asInstanceOf[Int]
    val numFeatures: Int = params.getOrElse("numFeatures", Math.pow(2, 16).toInt).asInstanceOf[Int]
    val minDocFreq: Int = params.getOrElse("minDocFreq", 0).asInstanceOf[Int]
    if(verbose)println(maxIter, numFeatures, minDocFreq)

    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val hashtf = new HashingTF().setNumFeatures(numFeatures).setInputCol("words").setOutputCol("tf")
    val idf = new IDF().setMinDocFreq(minDocFreq).setInputCol("tf").setOutputCol("features")
    //minDocFreq: remove sparse terms
    val targetToIndexedLabel = new StringIndexer().setHandleInvalid("keep").setInputCol("target").setOutputCol("label")
    val labels = new StringIndexer().setHandleInvalid("keep").setInputCol("target").setOutputCol("label").fit(trainDF).labels
    val lr = new LogisticRegression().setMaxIter(maxIter).setFeaturesCol("features").setLabelCol("label").setPredictionCol("predictedLabel")
    val indexedLabelToTarget = new IndexToString().setInputCol("predictedLabel").setOutputCol("unindexedLabel").setLabels(labels)
    val a = Array(tokenizer, hashtf, idf, targetToIndexedLabel, lr, indexedLabelToTarget)
    super.train(trainDF, params, stages ++ Array(tokenizer, hashtf, idf, targetToIndexedLabel, lr, indexedLabelToTarget))
  }
}