package com.enzobnl.annotweet.systems

import com.enzobnl.annotweet.utils.Utils
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.WrappedArray

class TF_IDF_RF_BasedTSA extends TweetSentimentAnalyzer {
  _options = this.options ++ Map(
    "useSmileysTreatment" -> true,
    "useButFilter" -> true,
    "butWord" -> "but",
    "usePunctuationTreatment" -> true,
    "punctuations" -> "\\.:,);(!?",
    "useIsTreatment" -> true,
    "useTabulationTreatment" -> true,
    "useHashtagsTreatment" -> true,
    "useFillersRemoving" -> true,
    "useWordsPairs" -> true,
    "fillers" -> Array("the",""," ","of","it","\t"), // csv
    "numFeatures" -> Math.pow(2, 16).toInt,
    "minDocFreq" -> 4
  )

  /**
    * Train system.
    * DEV: If verbose option is true: print some params review
    * @param trainDF : default: entire dataFrame
    * @return time elapsed
    */
  override def train(trainDF: DataFrame=defaultDF): Double = {
    Utils.getTime {
      if (options("verbose").asInstanceOf[Boolean]) println(options)
      var stages: Array[PipelineStage] = Array()
      // smileys treatments
      if(options("useSmileysTreatment").asInstanceOf[Boolean]) stages = stages :+ new SQLTransformer().setStatement(s"""SELECT id, target, regexp_replace(regexp_replace(text, ':[(]', ' sadsmiley '), ':[)]', ' happysmiley ') AS text FROM __THIS__""")
      // Punctuations treatment
      if(options("usePunctuationTreatment").asInstanceOf[Boolean]) stages = stages :+ new SQLTransformer().setStatement(s"""SELECT id, target, regexp_replace(text, '[${options("punctuations")}]', ' ') AS text FROM __THIS__""")
      // Is treatment TODO: all contractions
      if(options("useIsTreatment").asInstanceOf[Boolean]) stages = stages :+ new SQLTransformer().setStatement(s"""SELECT id, target, regexp_replace(text, '\\'s ', ' is ') AS text FROM __THIS__""")
      // tab treatment
      if(options("useTabulationTreatment").asInstanceOf[Boolean]) stages = stages :+ new SQLTransformer().setStatement(s"""SELECT id, target, regexp_replace(text, '\\t', ' ') AS text FROM __THIS__""")
      //Tokenization
      stages = stages :+ new Tokenizer().setInputCol("text").setOutputCol("words")
      // ButFilter
      if(options("useButFilter").asInstanceOf[Boolean]){
        val butWord = options("butWord").asInstanceOf[String]
        _spark.udf.register("bf", (wa: WrappedArray[String]) => wa.foldRight[WrappedArray[String]](WrappedArray.empty[String])((word: String, newWa: WrappedArray[String]) => if (newWa.nonEmpty && newWa(0) == butWord) newWa else word +: newWa).filter(_ != butWord))
        stages = stages :+ new SQLTransformer().setStatement(s"""SELECT id, target, text, bf(words) AS words FROM __THIS__""")
      }
      //fillersRemoving
      if(options("useFillersRemoving").asInstanceOf[Boolean]) {
        val fillers = options("fillers").asInstanceOf[Array[String]]
        _spark.udf.register("fr", (wa: WrappedArray[String]) => wa.filter(!fillers.contains(_)))
        stages = stages :+ new SQLTransformer().setStatement(s"""SELECT id, target, text, fr(words) AS words FROM __THIS__""")
      }
      //word pairs
      if(options("useWordsPairs").asInstanceOf[Boolean]) {
        _spark.udf.register("wp", (wa: WrappedArray[String]) => wa.foldLeft[WrappedArray[String]](WrappedArray.empty[String])((newWa: WrappedArray[String], word: String) => if (newWa.nonEmpty) (newWa(newWa.size - 1) + word) +: newWa :+ word else word +: newWa))
        stages = stages :+ new SQLTransformer().setStatement(s"""SELECT id, target, text, wp(words) AS words FROM __THIS__""")
      }
      //hashtag Treatment = removing '#' in tweet corpse and copy complete hashtags to the end of the tweet
      if(options("useHashtagsTreatment").asInstanceOf[Boolean]) {
        _spark.udf.register("htt", (wa: WrappedArray[String]) => wa.foldRight[WrappedArray[String]](WrappedArray.empty[String])((word: String, newWa: WrappedArray[String]) => if (word.startsWith("#")) word.substring(1) +: newWa :+ word else word +: newWa))
        stages = stages :+ new SQLTransformer().setStatement(s"""SELECT id, target, text, htt(words) AS words FROM __THIS__""")
      }
      //http://t.co/UT5GrRwAaA
      //TF-IDF
      stages = stages :+ new HashingTF().setNumFeatures(options("numFeatures").asInstanceOf[Int]).setInputCol("words").setOutputCol("tf")
      stages = stages :+ new IDF().setMinDocFreq(options("minDocFreq").asInstanceOf[Int]).setInputCol("tf").setOutputCol("features")
      stages = stages :+ new StringIndexer().setHandleInvalid("keep").setInputCol("target").setOutputCol("label")
      val labels = new StringIndexer().setHandleInvalid("keep").setInputCol("target").setOutputCol("label").fit(trainDF).labels
      
      //LR
      stages = stages :+ new RandomForestClassifier().setFeaturesCol("features").setLabelCol("label").setPredictionCol("predictedLabel")
      //revert back to string labels
      stages = stages :+ new IndexToString().setInputCol("predictedLabel").setOutputCol("unindexedLabel").setLabels(labels)

      val pipeline = new Pipeline().setStages(stages)
      this._pipelineModel = pipeline.fit(trainDF)
    }
  }
}
