package com.enzobnl.annotweet.system

import java.io.IOException

import com.enzobnl.annotweet.utils.QuickSQLContextFactory
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.{DataFrame, SQLContext}

trait TweetSentimentAnalyzer {
  private val MODELS_PATH: String = s"${this.getClass.getResource("/datas").getFile}/../models/"
  private val DATAS_PATH: String = s"${this.getClass.getResource("/datas").getFile}/"
  def getDataPath(datasetName: String): String = DATAS_PATH + datasetName


  protected lazy val _spark: SQLContext = QuickSQLContextFactory.getOrCreate("annotweet")
  protected var _pipelineModel: PipelineModel = null
  protected val _df: DataFrame = loadData("data.txt")


  def isTrained = _pipelineModel != null
  /**
    * Save trained model
    * @param modelID
    * @return
    */
  def save(modelID: String): Boolean = {
    try{
      this._pipelineModel.save(s"$MODELS_PATH$modelID")
      true
    } catch {
      case _: IOException => false
      case _: NotImplementedError => false // if model not trained yet

    }
  }

  /**
    * Load a saved model (saved with trained
    * @param modelID
    * @return
    */
  def load(modelID: String): Boolean = {
    try{
      this._pipelineModel = PipelineModel.read.load(s"$MODELS_PATH$modelID")
      true
    } catch{
      case _: Exception => false
    }
  }

  /**
    * Load dataset from /datas folder in resources folder
    * @param datasetName
    * @return
    */
  def loadData(datasetName: String): DataFrame = {
    _spark.read.option("delimiter", ")").csv(getDataPath("data.txt")).createOrReplaceGlobalTempView("temp")
    _spark.sql("""SELECT regexp_extract(_c0, '\\(([^,]*),(.*)', 1) AS id,regexp_extract(_c0, '\\(([^,]*),(.*)', 2) AS target,_c1 AS text FROM global_temp.temp""".stripMargin)
  }

  /**
    * Tag a single tweet text
    * @param tweet
    * @return
    */
  def tag(tweet: String): Tag.Value

  /**
    * Train system.
    * @param trainDF: default: entire dataFrame
    * @param params: system hyperparameters map
    * @return time elapsed
    */
  def train(trainDF: DataFrame=_df, params: Map[String, AnyVal]=Map()): Double

  /**
    * CrossValidation with nChunks the numbers of chunks made out of DATA_PATH dataset
    * @param nChunks: default is df.count() meaning that a "one out cross validation" will be performed
    * @return
    */
  def crossValidate(nChunks: Int=_df.count().toInt): Double ={
    if(nChunks <= 1) throw new IllegalArgumentException("nChunks must be > 1")
    // Split data in nChunks
    val dfs: Array[DataFrame] = loadData("data.txt").randomSplit((for (_ <- 1 to nChunks) yield 1.0).toList.toArray)
    // List that will be feed by accuracies (size will be nChunks
    var accuraciesList = List[Double]()
    // Evaluator (compare label column to predictedLabel
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("predictedLabel")
    // nChunks loops
    for(i <- 0 until nChunks){
      // build trainDF of size ~= (nChunks - 1)/nChunks
      var trainDF: DataFrame = dfs.foldLeft[(Int, DataFrame)]((0, null))((i_df: (Int, DataFrame), df: DataFrame) =>
        if (i_df._1 != i)
          (i_df._1 + 1, if(i_df._2 != null) i_df._2.union(df) else df)
        else (i_df._1 + 1, i_df._2))._2
      train(trainDF)
      // test on the i-th df in dfs of size ~= 1/nChunks and add evaluated accuracy to accuraciesList
      accuraciesList = accuraciesList :+ evaluator.evaluate(_pipelineModel.transform(dfs(i)))
    }
    // return mean accuracy
    accuraciesList.sum/nChunks
  }
}
