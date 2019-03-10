package com.enzobnl.annotweet.playground

import com.enzobnl.annotweet.systems.TweetSentimentAnalyzer
import com.enzobnl.annotweet.utils.Utils
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.DataFrame

class OnlyPreprocessingTSA extends TweetSentimentAnalyzer {
  _options = this.options ++ Map(
    "useButFilter" -> true,
    "butWord" -> " but ",
    "usePunctuationTreatment" -> true,
    "punctuations" -> "\\.",
    "useIsTreatment" -> true,
    "maxIter" -> 100,
    "numFeatures" -> Math.pow(2, 16).toInt,
    "minDocFreq" -> 0
  )

  /**
    * Train system.
    *
    * @param trainDF : default: entire dataFrame
    * @return time elapsed
    */
  override def train(trainDF: DataFrame=defaultDF): Double = {
    Utils.getTime {
      if (options("verbose").asInstanceOf[Boolean]) println(options)
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

      // Is treatment
      if(options("useIsTreatment").asInstanceOf[Boolean]) {
        val isTreatment: PipelineStage = new SQLTransformer().setStatement(s"""SELECT id, target, regexp_replace(text, '[${options("punctuations")}]', ' ') AS text FROM __THIS__""")
        stages = stages :+ isTreatment
      }
      //filter spaces
//      if(options("useMultiSpaceFiltering").asInstanceOf[Boolean]) {
//        val punctuationTreatment: PipelineStage = new SQLTransformer().setStatement(s"""SELECT id, target, regexp_replace(text, '[${options("punctuations")}]', ' ') AS text FROM __THIS__""")
//        stages = stages :+ punctuationTreatment
//      }
      //Tokenization
      val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")



      val pipeline = new Pipeline().setStages(stages ++ Array(tokenizer))

      this._pipelineModel = pipeline.fit(trainDF)
    }
  }
}
