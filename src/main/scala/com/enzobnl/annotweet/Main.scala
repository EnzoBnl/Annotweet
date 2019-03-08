package com.enzobnl.annotweet

import com.enzobnl.annotweet.systems.{TF_IDF_LR_BasedTSA, TweetSentimentAnalyzer}

object Main extends App {
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
  //expe(new TF_IDF_LR_BasedTSA(), "10")
  println(new TF_IDF_LR_BasedTSA().option("usePunctuationTreatment", false).crossValidate(10))
  println(new TF_IDF_LR_BasedTSA().crossValidate(10))

}
