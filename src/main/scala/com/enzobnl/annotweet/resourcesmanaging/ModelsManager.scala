package com.enzobnl.annotweet.resourcesmanaging


import org.apache.spark.ml.PipelineModel

/**
  * Used to make easier the saving and loading of models under resources folder
  */
object ModelsManager {
  val modelsPath = s"${this.getClass.getResource("/data").getFile}/../models"
  def getModelPath(modelID: String): String = s"$modelsPath/$modelID"
  def save(model: PipelineModel, modelID: String): Unit = model.save(s"$modelsPath/$modelID")
  def load(modelID: String): PipelineModel = PipelineModel.read.load(getModelPath(modelID))
}
