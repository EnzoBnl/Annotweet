package com.enzobnl.annotweet.systems

import com.enzobnl.annotweet.utils.QuickSQLContextFactory
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * A modelBuilder should be able to build a PipelineModel out of its options maps
  */
trait ModelBuilder {
  // SPARK Instance (shared among application
  protected lazy val _spark: SQLContext = QuickSQLContextFactory.getOrCreate("annotweet")

  // Options Map
  protected var _options: Map[String, Any] = Map[String, Any]("verbose" -> true)
  def option(key: String, value: Any): this.type = {
    _options += (key -> value); this}
  def options: Map[String, Any] = _options

  // COLUMNS
  protected val _labelCol: String = "label"
  protected val _predictionCol: String = "predictedLabel"
  protected  val _featuresCol = "features"
  def labelCol: String = _labelCol
  def predictionCol: String = _predictionCol
  def featuresCol: String = _featuresCol

  /**
    * This is the actual BUILD method to implement: This use the options to train a PipelineModel
    * @param trainDF: DataFrame on which we will train the model
    * @return trained model
    */
  def train(trainDF: DataFrame): PipelineModel

  def registerUDFs(): Unit

}
