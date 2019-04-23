package com.enzobnl.annotweet.systems

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.DataType

import scala.util.Random

class UnaryTransformerFromFunc[IN, OUT](inputCol: String, outputCol: String, dataType: DataType, f: IN => OUT, override val uid: String=Identifiable.randomUID("ngramtokenizer"))
  extends UnaryTransformer[IN, OUT, UnaryTransformerFromFunc[IN, OUT]]{
  setInputCol(inputCol)
  setOutputCol(outputCol)

  override protected def createTransformFunc: IN => OUT = f

  override protected def outputDataType: DataType = dataType
}