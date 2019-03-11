package com.enzobnl.annotweet.systems

import java.security.InvalidKeyException

import com.enzobnl.annotweet.systems.Tag.Value
import org.apache.spark.ml.classification.{ClassificationModel, Classifier, LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StringType}

import scala.collection.mutable
import scala.util.Random

/**
  * This factory aims at achieving the construction of TweetSentimentAnalyzerBuilders from a common preprocessing
  * based on TF & IDF transformations, and several other optional transformations and filters on strings & words
  */
object TF_IDF_BasedTSABuilderFactory {
  /**
    * Create an instance of TweetSentimentAnalyzerBuilder from 'classifier' algorithm pre-parametrized PipelineStage
    * and using a common preprocessing unit (TF IDF based + manipulations/filters)
    * @param classifier
    * @tparam FeaturesType
    * @tparam E
    * @tparam M
    * @return
    */
  def create[FeaturesType, E <: Classifier[FeaturesType, E, M], M <: ClassificationModel[FeaturesType, M]]
  (classifier: Classifier[FeaturesType, E, M]): TweetSentimentAnalyzerBuilder = {
    new TweetSentimentAnalyzerBuilder {
      this._options = this._options ++ Map(
        "useSmileysTreatment" -> true,
        "useButFilter" -> true,
        "butWord" -> "but",
        "usePunctuationTreatment" -> true,
        "punctuations" -> "\\.:,);(!?\\t\\'",
        "useHashtagsTreatment" -> true,
        "useFillersRemoving" -> true,
        "useWordsPairs" -> true,
        "fillers" -> List("the", "", " ", "of", "it", "\t", "a", "an", "his", "her", "theirs", "yours", "ours", "our", "him"),
        "numFeatures" -> Math.pow(2, 16).toInt,
        "minDocFreq" -> 4)

      override def trainOnCheckedDF(trainDF: DataFrame): PipelineModel = {
        if (options("verbose").asInstanceOf[Boolean]) println(options)
        var stages: Array[PipelineStage] = Array()
        // smileys treatments
        if (options("useSmileysTreatment").asInstanceOf[Boolean]) stages = stages :+ new SQLTransformer().setStatement(s"""SELECT $idCol, $targetTagCol, regexp_replace(regexp_replace($textCol, ':[(]', ' sadsmiley '), ':[)]', ' happysmiley ') AS $textCol FROM __THIS__""")
        // Punctuations treatment
        if (options("usePunctuationTreatment").asInstanceOf[Boolean]) stages = stages :+ new SQLTransformer().setStatement(s"""SELECT $idCol, $targetTagCol, regexp_replace(text, '[${options("punctuations")}]', ' ') AS $textCol FROM __THIS__""")
        //Tokenization
        stages = stages :+ new Tokenizer().setInputCol(s"$textCol").setOutputCol(wordsCol)
        // ButFilter
        if (options("useButFilter").asInstanceOf[Boolean]) {
          val butWord = options("butWord").asInstanceOf[String]
          _spark.udf.register("bf", (wa: mutable.WrappedArray[String]) => wa.foldRight[mutable.WrappedArray[String]](mutable.WrappedArray.empty[String])((word: String, newWa: mutable.WrappedArray[String]) => if (newWa.nonEmpty && newWa(0) == butWord) newWa else word +: newWa).filter(_ != butWord))
          stages = stages :+ new SQLTransformer().setStatement(s"""SELECT $idCol, $targetTagCol, $textCol, bf($wordsCol) AS $wordsCol FROM __THIS__""")
        }
        //fillersRemoving
        if (options("useFillersRemoving").asInstanceOf[Boolean]) {
          val fillers = options("fillers").asInstanceOf[List[String]]
          _spark.udf.register("fr", (wa: mutable.WrappedArray[String]) => wa.filter(!fillers.contains(_)))
          stages = stages :+ new SQLTransformer().setStatement(s"""SELECT $idCol, $targetTagCol, $textCol, fr($wordsCol) AS $wordsCol FROM __THIS__""")
        }
        //hashtag Treatment = removing '#' in tweet corpse and copy complete hashtags to the end of the tweet
        if (options("useHashtagsTreatment").asInstanceOf[Boolean]) {
          _spark.udf.register("htt", (wa: mutable.WrappedArray[String]) => wa.foldRight[mutable.WrappedArray[String]](mutable.WrappedArray.empty[String])((word: String, newWa: mutable.WrappedArray[String]) => if (word.startsWith("#")) word.substring(1) +: newWa :+ word else word +: newWa))
          stages = stages :+ new SQLTransformer().setStatement(s"""SELECT $idCol, $targetTagCol, $textCol, htt($wordsCol) AS $wordsCol FROM __THIS__""")
        }

        //word pairs
        if (options("useWordsPairs").asInstanceOf[Boolean]) {
          _spark.udf.register("wp", (wa: mutable.WrappedArray[String]) => wa.foldLeft[mutable.WrappedArray[String]](mutable.WrappedArray.empty[String])((newWa: mutable.WrappedArray[String], word: String) => if (newWa.nonEmpty && !word.startsWith("#")) (newWa(newWa.size - 1) + word) +: newWa :+ word else newWa :+ word))
          stages = stages :+ new SQLTransformer().setStatement(s"""SELECT $idCol, $targetTagCol, $textCol, wp($wordsCol) AS $wordsCol FROM __THIS__""")
        }

        //TF-IDF
        stages = stages :+ new HashingTF().setNumFeatures(options("numFeatures").asInstanceOf[Int]).setInputCol(wordsCol).setOutputCol("tf")
        stages = stages :+ new IDF().setMinDocFreq(options("minDocFreq").asInstanceOf[Int]).setInputCol("tf").setOutputCol(featuresCol)
        stages = stages :+ new StringIndexer().setHandleInvalid("keep").setInputCol(targetTagCol).setOutputCol(this.labelCol)
        val labels = new StringIndexer().setHandleInvalid("keep").setInputCol(targetTagCol).setOutputCol(this.labelCol).fit(trainDF).labels

        //ALGORITHM
        stages = stages :+ classifier.setFeaturesCol(this.featuresCol).setLabelCol(this.labelCol).setPredictionCol(this.predictionCol)
        //revert back to string labels
        stages :+ new IndexToString().setInputCol(this.predictionCol).setOutputCol(predictedTagCol).setLabels(labels)
        new Pipeline().setStages(stages).fit(trainDF)
      }
    }
  }
  def createWithLogisticRegression(maxIter: Int): TweetSentimentAnalyzerBuilder ={
    create(new LogisticRegression().setMaxIter(maxIter)).option("algorithmClass", "LR")
  }
  def createWithRandomForest(): TweetSentimentAnalyzerBuilder ={
    create(new RandomForestClassifier()).option("algorithmClass", "RF")
  }


}
