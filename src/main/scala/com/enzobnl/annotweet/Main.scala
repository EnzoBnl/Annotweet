package com.enzobnl.annotweet

import com.enzobnl.annotweet.resourcesmanaging.{ModelsManager, TweetDatasetsManager}
import com.enzobnl.annotweet.systems.TSABuilderFactory.Vectorizers
import com.enzobnl.annotweet.systems.{TSABuilderFactory, _}
import com.enzobnl.annotweet.utils.QuickSQLContextFactory
import com.enzobnl.annotweet.validation.TSACrossValidation
import org.apache.spark.ml.classification.GBTClassifier


object Main extends App {

  //Test preprocess for Khiops
  val spark = QuickSQLContextFactory.getOrCreate()
  //  val df = TweetDatasetsManager.loadDataset("vrai_final.txt", "id", "target", "text")
  val dfold = TweetDatasetsManager.loadDataset("tweets_annotes_promo.txt", "id", "target", "text")
  //  val dfair = TweetDatasetsManager.loadDataset("air.txt", "id", "target", "text")
  val tsa = TSABuilderFactory.createWithGBT(Some(250))
  tsa.registerUDFs()
  ModelsManager.load("final").transform(dfold).show(100)
//  ModelsManager.save(tsa.train(dfold), "final")
  System.exit(0)

  //  ModelsManager.load("final").transform(dfold).show()
  //  System.exit(0)
  val arraytsas = Array(
    Seq("useSmileysTreatment", "useButFilter", "useHashtagsTreatment","useFillersRemoving", "useWordsPairs", "useLinksTreatment").foldLeft(TSABuilderFactory.createWithGBT(Some(5)))((acc, e) => acc.option(e, false)),
      TSABuilderFactory.createWithGBT(Some(5))

  )
  val res = TSACrossValidation.crossValidate(dfold, arraytsas.asInstanceOf[Array[ModelBuilder]])
  println(res)
  ModelsManager.save(res._3, "best1")
}
