package com.enzobnl.annotweet

import java.nio.file.{Files, Path, Paths}
import com.enzobnl.annotweet.resourcesmanaging.{ModelsManager, TweetDatasetsManager}
import com.enzobnl.annotweet.systems.TSABuilderFactory
import com.enzobnl.annotweet.utils.Utils
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}

object Main extends App{
  if(this.args.length == 0) {
    println("Wrong number of arguments, script should be used like that: 'sudo annotweet score chemin/vers/tweets_a_annoter.txt chemin/vers/futur_tweets_annotes.txt' OR 'sudo annotweet train chemin/vers/tweets_annotes.txt'")
    System.exit(-1)
  }
  if(this.args(0) == "score"){
    if(this.args.length != 3) {
      println("Wrong number of arguments, script should be used for scoring like that: sudo annotweet score chemin/vers/tweets_a_annoter.txt chemin/vers/futur_tweets_annotes.txt")
      System.exit(-1)
    }
    val inputTweets = this.args(1)
    val outputTweets = this.args(2)
    println("Scoring started")
    val inputDF = TweetDatasetsManager.loadDataset(inputTweets, "id", "target", "text")
    TSABuilderFactory.createWithGBT(Some(250)).registerUDFs()
    val model = ModelsManager.load("final")
    val predicted = model.transform(inputDF)
    val formatted = predicted.withColumn("order", monotonically_increasing_id()).join(inputDF.selectExpr("id", "text as otext"), "id").orderBy(col("order")).dropDuplicates("order").selectExpr("CONCAT('(', id, ',', unindexedLabel, ') ', otext)").repartition(1)
    val tmpFolder = TweetDatasetsManager.getDatasetPath("tmp")
    formatted.write.text(tmpFolder)
    val file: String = Files.list(Paths.get(tmpFolder)).toArray.filter( file => file.toString.endsWith(".txt"))(0).asInstanceOf[Path].toString
    Utils.moveAndRename(file, outputTweets)
    Utils.deleteFolder(tmpFolder)
    println("Scoring ended")

  }
  else if(this.args(0) == "train"){
    if(this.args.length != 2) {
      println("Wrong number of arguments, script should be used for training like that: sudo annotweet train chemin/vers/tweets_annotes.txt")
      System.exit(-1)
    }
    val inputTweets = this.args(1)
    println("Training started")
    val inputDF = TweetDatasetsManager.loadDataset(inputTweets, "id", "target", "text")
    val tsa = TSABuilderFactory.createWithGBT(Some(250))
    tsa.registerUDFs()
    val model = tsa.train(inputDF)
    ModelsManager.rawSave(model, "newModel")
    println("Training ended")
  }
  else{
    println("annotweet.sh first argument must be either 'score' or 'train'")
  }
}