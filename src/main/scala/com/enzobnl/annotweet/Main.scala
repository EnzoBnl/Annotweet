package com.enzobnl.annotweet

import com.enzobnl.annotweet.resourcesmanaging.TweetDatasetsManager
import com.enzobnl.annotweet.systems._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, Row}



object Main extends App {
  def insight(tsa: TweetSentimentAnalyzerBuilder, df: DataFrame): Unit ={
    val model = tsa.train(df.limit(1))
    val row: Row = TweetProcessor(model, tsa).transformTweet("test tweet: nothing here but\t#now, :) :( it's,, the  https://t.co/egjegg87 #twwet:oioi:(, HeRe :) http://t.co/egjegg87; it's #goog airlines for sure :)!! ? http://t.co/egjegg87#looool http://t.co/egjegg87")
    println(">>>TEXT:")
    println(row.getAs[String]("text"))
    println(">>>WORDS:")
    println(row.getAs[String]("words"))
  }

  //Data
  val df = TweetDatasetsManager.loadDataset("air.txt", "id", "target", "text")

  //Test on a tweet

  val arraytsas = Array(
    TF_IDF_BasedTSABuilderFactory.createWithLogisticRegression(100)
      .option("useTrigrams", true),
    TF_IDF_BasedTSABuilderFactory.createWithLogisticRegression(100)
      .option("useTrigrams", false))
  arraytsas.map(insight(_, df))

//  System.exit(0)
  //Cross val
  println(TSACrossValidation.crossValidate(df, arraytsas.asInstanceOf[Array[ModelBuilder]])

  )


}
