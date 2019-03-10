package com.enzobnl.annotweet.systems

import java.io.IOException

import com.enzobnl.annotweet.utils.QuickSQLContextFactory
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object TSACrossValidation {
  /**
    * CrossValidation with nChunks the numbers of chunks made out of df.
    * Compare models
    * USAGE EXAMPLE:
    * TSACrossValidation.crossValidate(df, Array(
    * new TF_IDF_LR_BasedTSA().option("useSmileysTreatment", false)
    * .option("useButFilter", false)
    * .option("usePunctuationTreatment", false)
    * .option("useIsTreatment", false)
    * .option("useTabulationTreatment", false)
    * .option("useHashtagsTreatment", false)
    * .option("useFillersRemoving", false)
    * .option("useWordsPairs", false),
    * new TF_IDF_LR_BasedTSA()
    * .option("useWordsPairs", false),
    * new TF_IDF_LR_BasedTSA()))
    *
    * @param df
    * @param models
    * @param nChunks : default is df.count() meaning that a "one out cross validation" will be performed
    * @param verbose
    * @return best model options
    */
  def crossValidate(df: DataFrame, models: Array[TweetSentimentAnalyzer], nChunks: Int = 10, verbose: Boolean = true): (TSACrossValidationResult, Map[String, Any]) = {
    if (nChunks <= 1) throw new IllegalArgumentException("nChunks must be > 1")
    // Split data in nChunks
    val dfs: Array[DataFrame] = df.randomSplit((for (_ <- 1 to nChunks) yield 1.0).toList.toArray)
    // List that will be feed by accuracies (size will be nChunks
    var accuraciesList = List[Double]()
    // Evaluator (compare label column to predictedLabel
    val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy").setLabelCol("label").setPredictionCol("predictedLabel")
    var bestModel: (TSACrossValidationResult, Map[String, Any]) = (TSACrossValidationResult(0, 0), null)
    //models loop
    for (model <- models) {
      // nChunks loops
      for (i <- 0 until nChunks) {
        if (verbose) print(s"step ${i + 1}/$nChunks:::")
        // build trainDF of size ~= (nChunks - 1)/nChunks
        var trainDF: DataFrame = dfs.foldLeft[(Int, DataFrame)]((0, null))((i_df: (Int, DataFrame), df: DataFrame) =>
          if (i_df._1 != i)
            (i_df._1 + 1, if (i_df._2 != null) i_df._2.union(df) else df)
          else (i_df._1 + 1, i_df._2))._2
        model.train(trainDF)
        // test on the i-th df in dfs of size ~= 1/nChunks and add evaluated accuracy to accuraciesList
        val accuracy = evaluator.evaluate(model.pipelineModel.transform(dfs(i)))
        accuraciesList = accuraciesList :+ accuracy
        if (verbose) println("-->", accuracy)

      }
      // return mean accuracy
      val mean = accuraciesList.sum / nChunks
      val res = TSACrossValidationResult(mean, Math.sqrt(accuraciesList.foldLeft[Double](0)((acc: Double, t: Double) => acc + Math.pow(t - mean, 2)) / nChunks))
      if(res.mean > bestModel._1.mean) bestModel = (res, model.options)
      if (verbose) println((res, model.options))
      accuraciesList = List[Double]()
    }
    bestModel
  }
}
