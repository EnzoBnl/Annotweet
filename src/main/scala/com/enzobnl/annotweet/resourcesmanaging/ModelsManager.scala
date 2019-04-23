package com.enzobnl.annotweet.resourcesmanaging


import java.nio.file.Files

import org.apache.spark.ml.PipelineModel

/**
  * Used to make easier the saving and loading of models under resources folder
  */
object ModelsManager {
  val modelsPath = s"src/main/resources/models"
  def getModelPath(modelID: String): String = s"$modelsPath/$modelID"
  def save(model: PipelineModel, modelID: String): Unit = model.save(getModelPath(modelID))
  def rawSave(model: PipelineModel, modelID: String): Unit = model.save(modelID)

  def load(modelID: String): PipelineModel = {
    try{
      PipelineModel.read.load(getModelPath(modelID))
    }
    catch {
      case _: Exception => PipelineModel.read.load(modelID)
    }
  }
}
