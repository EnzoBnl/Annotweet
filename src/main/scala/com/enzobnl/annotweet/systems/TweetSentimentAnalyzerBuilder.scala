package com.enzobnl.annotweet.systems

import java.security.InvalidKeyException

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.types._
/**
  * A tweetSentimentAnalyzerBuilder is a ModelBuilder that add some restrictions about the train DataFrame Structure
  */
trait TweetSentimentAnalyzerBuilder extends ModelBuilder{
  //Columns
  protected val _idCol = "id" // tweet id
  protected val _targetTagCol = "target" // Tag
  protected val _textCol = "text" // tweet text
  protected val _predictedTagCol = "unindexedLabel"
  protected val _wordsCol = "words"
  def wordsCol: String = _wordsCol
  def idCol: String = _idCol
  def targetTagCol: String = _targetTagCol
  def textCol: String = _textCol
  def predictedTagCol: String = _predictedTagCol

  /**
    * Check if the columns contains id, targetTag and text columns names typed as StringType
    * @param columns
    */
  def checkTrainDFCols(columns: StructType): Unit = {
    if(!columns.contains(StructField(idCol, StringType))) throw new InvalidKeyException(s"Train DataFrame columns ${columns.toList} must contain '$idCol'")
    if(!columns.contains(StructField(targetTagCol, StringType))) throw new InvalidKeyException(s"Train DataFrame columns ${columns.toList} must contain '$targetTagCol'")
    if(!columns.contains(StructField(textCol, StringType))) throw new InvalidKeyException(s"Train DataFrame columns ${columns.toList} must contain '$textCol'")
  }

  /**
    * This is an implementation that only check for DataFrame structure validity and then call abstract trainOnCheckedDF
    * @param trainDF: DataFrame on which we will train the model
    * @return trained model
    */
  override def train(trainDF: DataFrame): PipelineModel = {
    checkTrainDFCols(trainDF.schema)
    trainOnCheckedDF(trainDF)
  }

  /**
    * In this method, subclasses can assume that trainDF contains id, target and text columns and train the model
    * @param trainDF: DataFrame on which we will train the model
    * @return trained model
    */
  def trainOnCheckedDF(trainDF: DataFrame): PipelineModel

}


