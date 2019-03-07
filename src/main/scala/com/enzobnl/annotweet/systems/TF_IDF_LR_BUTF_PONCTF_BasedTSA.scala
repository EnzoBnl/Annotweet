package com.enzobnl.annotweet.systems

import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.sql.DataFrame

class TF_IDF_LR_BUTF_PONCTF_BasedTSA extends TF_IDF_LR_BUTF_BasedTSA {
  private val PONCTS: String = "\\."
  override def train(trainDF: DataFrame=_df, params: Map[String, AnyVal]=Map(), stages: Array[PipelineStage]=Array()): Double = {
    val ponctFilter: PipelineStage = new SQLTransformer()
      .setStatement(s"""SELECT id, target, regexp_replace(text, '[$PONCTS]', '') AS text FROM __THIS__""")
    super.train(trainDF, params,  Array(ponctFilter) ++ stages)
  }
}