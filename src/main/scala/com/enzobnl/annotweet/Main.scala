package com.enzobnl.annotweet

import com.enzobnl.annotweet.system.TF_IDF_LR_BasedTSA

import scala.util.Random

object Main extends App {
  Random.setSeed(System.currentTimeMillis())


  val model1 = new TF_IDF_LR_BasedTSA()
//  val modelID: String = "9" //Random.nextInt().toString
//  println(model1.train(modelID))
//  model1.load(modelID)
  println(model1.crossValidate(2))
  println(model1.crossValidate(4))
  println(model1.crossValidate(15))

  //  println(model1.tag("Renaissance de #macron"))
//  println(model1.tag("par-dessus la tête #macron"))

}