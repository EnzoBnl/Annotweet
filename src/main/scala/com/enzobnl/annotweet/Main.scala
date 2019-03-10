package com.enzobnl.annotweet

import com.enzobnl.annotweet.playground.OnlyPreprocessingTSA
import com.enzobnl.annotweet.systems._
import com.enzobnl.annotweet.utils.QuickSQLContextFactory
import org.apache.spark.sql.{DataFrame, Row}
import sun.text.normalizer.NormalizerBase.QuickCheckResult

import scala.collection.mutable


object Main extends App {

//  val spark = QuickSQLContextFactory.getOrCreate()
//  val m = Map("0" -> "Neg", "2" -> "Neu", "4" -> "Pos")
//  spark.udf.register("toTag", (s:String) => m(s))
//  spark.read.csv("c:/Prog/Java/Annotweet/Annotweet/src/main/resources/data/sent140.csv")
//    .selectExpr("CONCAT('(', _c1, ',', toTag(_c0), ') ', _c5) AS c").orderBy(org.apache.spark.sql.functions.rand()).repartition(1).write.text("c:/Prog/Java/Annotweet/Annotweet/src/main/resources/data/sent140")
//
//  System.exit(0)
  def tagThem(model: TweetSentimentAnalyzer, tweets: Array[String]) ={
    for(tweet <- tweets){
      println(model.tag(tweet))
    }
  }
  def expe(model: TweetSentimentAnalyzer, modelID: String) = {
    model.loadOrTrain(modelID)
    model.save(modelID)
    tagThem(model, Array("@united kudos for not Cancelled Flightling flights from DFW this morning.   United usually first to panic...",
      "thought so bad but finally so good",
      "@VirginAmerica got a flight (we were told) for 4:50 today..,checked my email and its for 4;50 TOMORROW. This is unacceptable.",
      "@united Luckily I made my flights this time but was so disappointed with the lack of communication :("))
  }
  val tsa: TweetSentimentAnalyzer = new TF_IDF_LR_BasedTSA().option("useTabulationTreatment", false).option("punctuations", "\\.:,);(!?\\t")
  val df = tsa.loadDataset("air.txt")
  tsa.train(df.limit(100))
  val row: Row = tsa.transformTweet("test tweet: nothing here but\t#now, it's,, the #twwet:oioi:(, here :) http://t.co/egjegg87 . it's #goog airlines for sure :)!! ? #looool")
  println(">>>TEXT:")
  println(row.getAs[String]("text"))
  println(">>>WORDS:")
  println(row.getAs[String]("words"))
  println(TSACrossValidation.crossValidate(df, Array(
    tsa,
    new TF_IDF_LR_BasedTSA())))  //CV
//  println(
//    TSACrossValidation.crossValidate(df, Array(
//    new TF_IDF_LR_BasedTSA().option("useSmileysTreatment", false)
//      .option("useButFilter", false)
//      .option("usePunctuationTreatment", false)
//      .option("useIsTreatment", false)
//      .option("useTabulationTreatment", false)
//      .option("useHashtagsTreatment", false)
//      .option("useFillersRemoving", false)
//      .option("useWordsPairs", false),
//    new TF_IDF_LR_BasedTSA()
//      .option("useWordsPairs", false),
//    new TF_IDF_LR_BasedTSA()))
//  )


  //expe(new TF_IDF_LR_BasedTSA(), "10")
//  println(new TF_IDF_LR_BasedTSA().option("useButFilter", false).crossValidate(10))
//  println(new TF_IDF_LR_BasedTSA().option("usePunctuationTreatment", false).crossValidate(10))
//  println(new TF_IDF_LR_BasedTSA().crossValidate(10))

}
