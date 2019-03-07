package com.enzobnl.annotweet

import com.enzobnl.annotweet.systems.{TF_IDF_LR_BUTF_BasedTSA, TF_IDF_LR_BUTF_PONCTF_BasedTSA, TF_IDF_LR_BasedTSA, TweetSentimentAnalyzer}
import com.enzobnl.annotweet.utils.QuickSQLContextFactory
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions._

import scala.util.Random

object Main extends App {

//  Random.setSeed(System.currentTimeMillis())

  def tagThem(model: TweetSentimentAnalyzer, tweets: Array[String]) ={
    for(tweet <- tweets){
      println(model.tag(tweet))
    }
  }

  val model0 = new TF_IDF_LR_BasedTSA()
  println(model0.train())
  tagThem(model0, Array("@united kudos for not Cancelled Flightling flights from DFW this morning.   United usually first to panic...",
  "thought so bad but finally so good",
    "@VirginAmerica got a flight (we were told) for 4:50 today..,checked my email and its for 4;50 TOMORROW. This is unacceptable.",
    "@united Luckily I made my flights this time but was so disappointed with the lack of communication :("))

  val model1 = new TF_IDF_LR_BUTF_BasedTSA()
  println(model1.train())
  tagThem(model1, Array("@united kudos for not Cancelled Flightling flights from DFW this morning.   United usually first to panic...",
    "thought so bad but finally so good",
    "@VirginAmerica got a flight (we were told) for 4:50 today..,checked my email and its for 4;50 TOMORROW. This is unacceptable.",
    "@united Luckily I made my flights this time but was so disappointed with the lack of communication :("))

  val model2 = new TF_IDF_LR_BUTF_PONCTF_BasedTSA()
  println(model2.train())
  tagThem(model2, Array("@united kudos for not Cancelled Flightling flights from DFW this morning.   United usually first to panic...",
    "thought so bad but finally so good",
    "@VirginAmerica got a flight (we were told) for 4:50 today..,checked my email and its for 4;50 TOMORROW. This is unacceptable.",
    "@united Luckily I made my flights this time but was so disappointed with the lack of communication :("))
}

//  println(model1.crossValidate(10, Map("minDocFreq"->2, "maxIter"->100)))

//  println(model1.crossValidate(15))
//  println(model1.crossValidate(15, Map("minDocFreq"->0)))


//  println(model1.tag("Renaissance de #macron"))
//  println(model1.tag("par-dessus la tête #macron"))
/*
val spark = QuickSQLContextFactory.getOrCreate("processor")
val df = spark.read.option("header", true).option("delimiter", ",")
  .csv("C:/Prog/Java/Annotweet/Annotweet/src/main/resources/datas/Tweets.csv").createOrReplaceGlobalTempView("t")
val replaceUDF: String => String = _.replace("\\n", " ; ")
spark.udf.register("replaceUDF", udf(replaceUDF))
spark.sql("""SELECT CONCAT('(', tweet_id, ',', upper(SUBSTRING(airline_sentiment, 1, 1)), SUBSTRING(airline_sentiment, 2, 2), ') ', REGEXP_REPLACE(text, '\n', ' ; ')) AS c FROM global_temp.t """)
    .repartition(1).filter("NOT(ISNULL(c))").write.text("C:/Prog/Java/Annotweet/Annotweet/src/main/resources/datas/airline.txt")

System.exit(0)*/