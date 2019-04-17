package com.enzobnl.annotweet.systems

import org.apache.spark.ml.classification.{ClassificationModel, Classifier, GBTClassifier, LogisticRegression, OneVsRest, RandomForestClassifier}
import org.apache.spark.ml.{classification, _}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.util.Random

/**
  * This factory aims at achieving the construction of TweetSentimentAnalyzerBuilders from a common preprocessing
  * based on TF & IDF transformations, and several other optional transformations and filters on strings & words
  */
object TSABuilderFactory {
  object Vectorizers extends Enumeration{
    val TF_IDF, WORD2VEC = Value
  }
  /**
    * Create an instance of TweetSentimentAnalyzerBuilder from 'classifier' algorithm pre-parametrized PipelineStage
    * and using a common preprocessing unit (TF IDF based + manipulations/filters)
    *
    * @param classifier
    * @tparam FeaturesType
    * @tparam E
    * @tparam M
    * @return
    */
  def create[FeaturesType, E <: Classifier[FeaturesType, E, M], M <: ClassificationModel[FeaturesType, M]]
  (classifier: Classifier[FeaturesType, E, M]): TweetSentimentAnalyzerBuilder = {
    new TweetSentimentAnalyzerBuilder {
      // TODO: add new markers to keep capitalization information: MOTX , bla ;.. __motXMaj__ __blaMin__
      this._options = this._options ++ Map(
        "algorithmClass" -> {if(classifier!=null) classifier.getClass.getName.substring(classifier.getClass.getName.lastIndexOf(".")+1) else null},
        "vectorizer" -> Vectorizers.WORD2VEC,
        "useSmileysTreatment" -> true,
        "useButFilter" -> true,
        "butWord" -> "mais",
        "usePunctuationIsolation" -> true,
        "usePunctuationDeletion" -> false,  // TODO: mettre que les virgules ?
        "punctuationsI" -> "\\.:,\\);\\(!?\\t\\'",
        "punctuationsD" -> ",\\t",
        "useHashtagsTreatment" -> true,
        "useFillersRemoving" -> true,
        "fillers" -> List("", " ", "\t"), // List("the", "", " ", "of", "it", "\t", "a", "an", "his", "her", "theirs", "yours", "ours", "our", "him"),
        "useWordsPairs" -> true,
        "useTrigrams" -> false, //TODO: upgrade it, in state it is bad for performances
        "useLinksTreatment" -> true, //true (best): treat, false: filter out links, null: do nothing (hard to interpret the result)
        "numFeatures" -> Math.pow(2, 16).toInt,
        "minDocFreq" -> 4,
        "w2vMaxIter" -> 45)
      override def registerUDFs(): Unit = {
        val butWord = options("butWord").asInstanceOf[String]
        val fillers = options("fillers").asInstanceOf[List[String]]

        _spark.udf.register("bf", (wa: mutable.WrappedArray[String]) => wa.foldRight[mutable.WrappedArray[String]](mutable.WrappedArray.empty[String])((word: String, newWa: mutable.WrappedArray[String]) => if (newWa.nonEmpty && newWa(0).toLowerCase == butWord) newWa else word +: newWa).filter(_.toLowerCase != butWord))
        _spark.udf.register("wp", (wa: mutable.WrappedArray[String]) => wa.foldLeft[mutable.WrappedArray[String]](mutable.WrappedArray.empty[String])((newWa: mutable.WrappedArray[String], word: String) => if (newWa.nonEmpty && !word.startsWith("#")) (newWa(newWa.size - 1) + word) +: newWa :+ word else newWa :+ word))
        _spark.udf.register("tg", (wa: mutable.WrappedArray[String]) => wa.foldRight[mutable.WrappedArray[String]](mutable.WrappedArray.empty[String])((word: String, newWa: mutable.WrappedArray[String]) => if (word.startsWith("#") || word.length < 4) word +: newWa else word +: (newWa ++ (for (i <- 3 to word.length) yield word.substring(i - 3, i)))))
        _spark.udf.register("fr", (wa: mutable.WrappedArray[String]) => wa.filter(!fillers.contains(_)))
        _spark.udf.register("htt", (wa: mutable.WrappedArray[String]) => wa.foldRight[mutable.WrappedArray[String]](mutable.WrappedArray.empty[String])((word: String, newWa: mutable.WrappedArray[String]) => if (word.startsWith("#")) word.substring(1) +: newWa :+ word else word +: newWa))
      }
      override def trainOnCheckedDF(trainDF: DataFrame): PipelineModel = {
        if (options("verbose").asInstanceOf[Boolean]) println(options)
        var stages: Array[PipelineStage] = Array()
        // smileys treatments
        if (options("useSmileysTreatment").asInstanceOf[Boolean]) stages = stages :+ new SQLTransformer().setStatement(s"""SELECT $idCol, $targetTagCol, regexp_replace(regexp_replace($textCol, ':[(]', ' sadsmiley '), ':[)]', ' happysmiley ') AS $textCol FROM __THIS__""")
        if (options("useLinksTreatment") != null) {
          // http links treatments'http://t.co/UT5GrRwAaA' need to become : 'http httpUT5GrRwAaA' (to keep 2 informations separately: there is a link and the exact link (maybe shared by different tweets)
          if (options("useLinksTreatment").asInstanceOf[Boolean]) stages = stages :+ new SQLTransformer().setStatement(s"""SELECT $idCol, $targetTagCol, regexp_replace($textCol, 'https?://t\\.co/[a-zA-Z0-9]*', CONCAT('http', ' //t ', regexp_extract($textCol, 'https?://t\\.co/([a-zA-Z0-9]*)', 1), ' ')) AS $textCol FROM __THIS__""")
          // remove links
          else stages = stages :+ new SQLTransformer().setStatement(s"""SELECT $idCol, $targetTagCol, regexp_replace($textCol, 'https?://t\\.co/[a-zA-Z0-9]*', ' ') AS $textCol FROM __THIS__""")
        }
        // Punctuations treatment
        if (options("usePunctuationIsolation").asInstanceOf[Boolean]) stages = stages :+ new SQLTransformer().setStatement(s"""SELECT $idCol, $targetTagCol, regexp_replace(text, '([${options("punctuationsI")}])', ' """ + """$1""" + s""" ') AS $textCol FROM __THIS__""")
        // Punctuations treatment
        if (options("usePunctuationDeletion").asInstanceOf[Boolean]) stages = stages :+ new SQLTransformer().setStatement(s"""SELECT $idCol, $targetTagCol, regexp_replace(text, '[${options("punctuationsD")}]', ' ') AS $textCol FROM __THIS__""")
        //Tokenization
        stages = stages :+ new Tokenizer().setInputCol(s"$textCol").setOutputCol(wordsCol)
        // ButFilter
        if (options("useButFilter").asInstanceOf[Boolean]) {
          stages = stages :+ new SQLTransformer().setStatement(s"""SELECT $idCol, $targetTagCol, $textCol, bf($wordsCol) AS $wordsCol FROM __THIS__""")
        }
        // fillersRemoving
        if (options("useFillersRemoving").asInstanceOf[Boolean]) {
          stages = stages :+ new SQLTransformer().setStatement(s"""SELECT $idCol, $targetTagCol, $textCol, fr($wordsCol) AS $wordsCol FROM __THIS__""")
        }
        //hashtag Treatment = removing '#' in tweet corpse and copy complete hashtags to the end of the tweet
        if (options("useHashtagsTreatment").asInstanceOf[Boolean]) {
          stages = stages :+ new SQLTransformer().setStatement(s"""SELECT $idCol, $targetTagCol, $textCol, htt($wordsCol) AS $wordsCol FROM __THIS__""")
        }
        //trigram: [word] -> [word, wor, ord] (don't apply on hashtags
        if (options("useTrigrams").asInstanceOf[Boolean]) {
          stages = stages :+ new SQLTransformer().setStatement(s"""SELECT $idCol, $targetTagCol, $textCol, tg($wordsCol) AS $wordsCol FROM __THIS__""")
        }
        //word pairs
        if (options("useWordsPairs").asInstanceOf[Boolean]) {
          stages = stages :+ new SQLTransformer().setStatement(s"""SELECT $idCol, $targetTagCol, $textCol, wp($wordsCol) AS $wordsCol FROM __THIS__""")
        }
        if (classifier != null) {
          options("vectorizer") match {
            case Vectorizers.TF_IDF => {
              //TF-IDF (not case sensitive !)
              stages = stages :+ new HashingTF().setNumFeatures(options("numFeatures").asInstanceOf[Int]).setInputCol(wordsCol).setOutputCol("tf")
              stages = stages :+ new IDF().setMinDocFreq(options("minDocFreq").asInstanceOf[Int]).setInputCol("tf").setOutputCol(featuresCol)
            }
            case Vectorizers.WORD2VEC => {
              stages = stages :+ new Word2Vec().setMaxIter(options("w2vMaxIter").asInstanceOf[Int])
                /*.setVectorSize(options("w2vVectorSize").asInstanceOf[Int])*/.setInputCol(wordsCol).setOutputCol(featuresCol)
            }
          }
          stages = stages :+ new StringIndexer().setHandleInvalid("keep").setInputCol(targetTagCol).setOutputCol(this.labelCol)
          val labels = new StringIndexer().setHandleInvalid("keep").setInputCol(targetTagCol).setOutputCol(this.labelCol).fit(trainDF).labels

          //ALGORITHM
          stages = stages :+ (
            classifier match {
              case c: GBTClassifier => {
                new OneVsRest().setClassifier(c).setFeaturesCol(this.featuresCol).setLabelCol(this.labelCol).setPredictionCol(this.predictionCol)
            }
              case _ => classifier.setFeaturesCol(this.featuresCol).setLabelCol(this.labelCol).setPredictionCol(this.predictionCol)
            }
            )
          //revert back to string labels
          stages = stages :+ new IndexToString().setInputCol(this.predictionCol).setOutputCol(predictedTagCol).setLabels(labels)
        }
        new Pipeline().setStages(stages).fit(trainDF)
      }
    }
  }
  def createWith[FeaturesType, E <: Classifier[FeaturesType, E, M], M <: ClassificationModel[FeaturesType, M]]
  (classifier: Classifier[FeaturesType, E, M]) = {
    create(classifier)
  }
  def createWithLogisticRegression(maxIter: Option[Int]=Some(75)): TweetSentimentAnalyzerBuilder = {
    if(maxIter.isEmpty) {
      create(new LogisticRegression()).option("maxIterClassifier", maxIter)
    }
    else {
      create(new LogisticRegression().setMaxIter(maxIter.get)).option("maxIterClassifier", maxIter)

    }
  }
  def createWithGBT(maxIter: Option[Int]=Some(100)): TweetSentimentAnalyzerBuilder = {
    if(maxIter.isEmpty){
      create(new GBTClassifier().setStepSize(0.15)).option("maxIterClassifier", maxIter)
    }
    else {
      create(new GBTClassifier().setMaxIter(maxIter.get).setStepSize(0.15)).option("maxIterClassifier", maxIter)

    }
  }
  def createWithRandomForest(): TweetSentimentAnalyzerBuilder = {
    create(new RandomForestClassifier())
  }
  def createTokenizer(): TweetSentimentAnalyzerBuilder = {
    create(null)
  }
}
