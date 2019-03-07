package com.enzobnl.annotweet.systems

import com.enzobnl.annotweet.utils.Utils
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature._
import org.apache.spark.sql.DataFrame

class TF_IDF_LR_BUTF_BasedTSA extends TF_IDF_LR_BasedTSA {
  private val BUT_WORD: String = " but "
  override def train(trainDF: DataFrame=_df, params: Map[String, AnyVal]=Map(), stages: Array[PipelineStage]=Array()): Double = {
  val butFilter: PipelineStage = new SQLTransformer()
    .setStatement(s"""SELECT id, target, substring_index(text, '$BUT_WORD', -1) AS text FROM __THIS__""")
  super.train(trainDF, params, Array(butFilter) ++ stages)
  }
}