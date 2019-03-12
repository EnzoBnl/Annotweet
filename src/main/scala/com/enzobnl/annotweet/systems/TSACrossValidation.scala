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
    * @param modelBuilders
    * @param nChunks : default is df.count() meaning that a "one out cross validation" will be performed
    * @param verbose
    * @return best model options
    */
  def crossValidate(df: DataFrame, modelBuilders: Array[ModelBuilder], nChunks: Int = 10, metricName: String = "accuracy", verbose: Boolean = true): (TSACrossValidationResult, Map[String, Any]) = {
    if (nChunks <= 1) throw new IllegalArgumentException("nChunks must be > 1")
    // Split data in nChunks
    val dfsTest: Array[DataFrame] = df.randomSplit((for (_ <- 1 to nChunks) yield 1.0).toList.toArray)
    // List that will be feed by accuracies (size will be nChunks
    var accuraciesList = List[Double]()
    // Evaluator (compare label column to predictedLabel
    val evaluator = new MulticlassClassificationEvaluator().setMetricName(metricName)
    var bestModel: (TSACrossValidationResult, Map[String, Any]) = (TSACrossValidationResult(0, 0), null)
    //df splits: all the df but the test chunk
    val dfsTrain: Array[DataFrame]= (for(i <- 0 until nChunks) yield dfsTest.foldLeft[(Int, DataFrame)]((0, null))((i_df: (Int, DataFrame), df: DataFrame) => if (i_df._1 != i) (i_df._1 + 1, if (i_df._2 != null) i_df._2.union(df) else df) else (i_df._1 + 1, i_df._2))._2).toArray

    //models loop
    for (modelBuilder <- modelBuilders) {
      // nChunks loops
      for (i <- 0 until nChunks) {
        if (verbose) print(s"step ${i + 1}/$nChunks:::")
        // build trainDF of size ~= (nChunks - 1)/nChunks

        val model = modelBuilder.train(dfsTrain(i))
        // test on the i-th df in dfs of size ~= 1/nChunks and add evaluated accuracy to accuraciesList
        evaluator.setLabelCol(modelBuilder.labelCol).setPredictionCol(modelBuilder.predictionCol)
        val accuracy = evaluator.evaluate(model.transform(dfsTest(i)))
        accuraciesList = accuraciesList :+ accuracy
        if (verbose) println("-->", accuracy)

      }
      // return mean accuracy
      val mean = accuraciesList.sum / nChunks
      val res = TSACrossValidationResult(mean, Math.sqrt(accuraciesList.foldLeft[Double](0)((acc: Double, t: Double) => acc + Math.pow(t - mean, 2)) / nChunks))
      if(res.mean > bestModel._1.mean) bestModel = (res, modelBuilder.options)
      if (verbose) println((res, modelBuilder.options))
      accuraciesList = List[Double]()
    }
    bestModel
  }
}
