package com.enzobnl.annotweet.systems

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature._
import com.enzobnl.annotweet.utils.Utils
import org.apache.spark.sql.DataFrame
import org.glassfish.jersey.process.internal.Stages

class TF_IDF_LR_BasedTSA extends TweetSentimentAnalyzer {
  private var options: Map[String, Any] = Map(
    "useButFilter" -> true,
    "butWord" -> " but ",
    "usePunctuationTreatment" -> true,
    "punctuations" -> "\\.",
    "maxIter" -> 100,
    "numFeatures" -> Math.pow(2, 16).toInt,
    "minDocFreq" -> 0
  )
  def option(key: String, value: Any): TF_IDF_LR_BasedTSA = {
    options += (key -> value)
    this
  }
  
  override def tag(tweet: String): Tag.Value = Tag.get(_pipelineModel.transform(tweetToDF(tweet)).select("unindexedLabel").collect()(0).getAs[String]("unindexedLabel"))

  override def train(trainDF: DataFrame=_df): Double = {
    Utils.getTime {
      if (verbose) println(options("maxIter"), options("numFeatures"), options("minDocFreq"))
      var stages: Array[PipelineStage] = Array()

      // ButFilter
      if(options("useButFilter").asInstanceOf[Boolean]){
        val butFilter: PipelineStage = new SQLTransformer().setStatement(s"""SELECT id, target, substring_index(text, '${options("butWord")}', -1) AS text FROM __THIS__""")
        stages = stages :+ butFilter
      }
      
      // Punctuations treatment
      if(options("usePunctuationTreatment").asInstanceOf[Boolean]) {
        val punctuationTreatment: PipelineStage = new SQLTransformer().setStatement(s"""SELECT id, target, regexp_replace(text, '[${options("punctuations")}]', ' ') AS text FROM __THIS__""")
        stages = stages :+ punctuationTreatment
      }
      
      //Tokenization
      val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
      
      //TF-IDF
      val hashtf = new HashingTF().setNumFeatures(options("numFeatures").asInstanceOf[Int]).setInputCol("words").setOutputCol("tf")
      val idf = new IDF().setMinDocFreq(options("minDocFreq").asInstanceOf[Int]).setInputCol("tf").setOutputCol("features")
      val targetToIndexedLabel = new StringIndexer().setHandleInvalid("keep").setInputCol("target").setOutputCol("label")
      val labels = new StringIndexer().setHandleInvalid("keep").setInputCol("target").setOutputCol("label").fit(trainDF).labels
      
      //LR
      val lr = new LogisticRegression().setMaxIter(options("maxIter").asInstanceOf[Int]).setFeaturesCol("features").setLabelCol("label").setPredictionCol("predictedLabel")
      val indexedLabelToTarget = new IndexToString().setInputCol("predictedLabel").setOutputCol("unindexedLabel").setLabels(labels)
      val pipeline = new Pipeline().setStages(stages ++ Array(tokenizer, hashtf, idf, targetToIndexedLabel, lr, indexedLabelToTarget))
      
      this._pipelineModel = pipeline.fit(trainDF)
    }
  }
}