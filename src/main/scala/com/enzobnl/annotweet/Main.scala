package com.enzobnl.annotweet

import com.enzobnl.annotweet.resourcesmanaging.TweetDatasetsManager
import com.enzobnl.annotweet.systems._
import org.apache.spark.sql.Row



object Main extends App {
  //Data
  val df = TweetDatasetsManager.loadDataset("air.txt", "id", "target", "text")

  //Test on a tweet
  val tsa: TweetSentimentAnalyzerBuilder = TF_IDF_BasedTSABuilderFactory.createWithLogisticRegression(100)
  val modelTest = tsa.train(df.limit(100))
  val row: Row = TweetProcessor(modelTest, tsa).transformTweet("test tweet: nothing here but\t#now, it's,, the #twwet:oioi:(, here :) http://t.co/egjegg87 . it's #goog airlines for sure :)!! ? #looool")
  println(">>>TEXT:")
  println(row.getAs[String]("text"))
  println(">>>WORDS:")
  println(row.getAs[String]("words"))
  System.exit(0)
  //Cross val
  println(TSACrossValidation.crossValidate(df,
    Array(
      TF_IDF_BasedTSABuilderFactory.createWithRandomForest,
      tsa
      )
    )
  )


}
