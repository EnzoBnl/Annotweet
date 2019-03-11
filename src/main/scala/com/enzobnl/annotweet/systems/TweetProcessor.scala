package com.enzobnl.annotweet.systems

import com.enzobnl.annotweet.utils.QuickSQLContextFactory
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Contains methods to tag a single tweet unlabelled with a model 'model' output by the builder 'tsaModelBuilder'
  * @param model
  * @param tsaModelBuilder
  */
case class TweetProcessor(model: PipelineModel, tsaModelBuilder: TweetSentimentAnalyzerBuilder) {
  private lazy val _spark = QuickSQLContextFactory.getOrCreate()
  /**
    * Create single line df from unlabelled row tweet text
    * @param tweet
    * @return single lined df [id, target, test]
    */
  private def tweetToDF(tweet: String): DataFrame = _spark.createDataFrame(Seq(Tuple3("0", "???", tweet))).toDF("id", "target", "text")

  /**
    * Tag a single tweet text
    * @param tweet
    * @return tweet tag
    */
  def tag(tweet: String): Tag.Value = Tag.get(model.transform(tweetToDF(tweet)).select(tsaModelBuilder.predictedTagCol).collect()(0).getAs[String]("unindexedLabel"))

  /**
    * Transform a single tweet embedded in single line dataframe
    * @param tweet
    * @return transformed Row
    */
  def transformTweet(tweet: String): Row = model.transform(tweetToDF(tweet)).collect()(0)
}
