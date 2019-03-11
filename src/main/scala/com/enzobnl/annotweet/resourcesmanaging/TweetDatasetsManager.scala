package com.enzobnl.annotweet.resourcesmanaging

import com.enzobnl.annotweet.utils.QuickSQLContextFactory
import org.apache.spark.sql.DataFrame

/**
  * Used to make easier the loading of datasets under resources folder
  */
object TweetDatasetsManager {
  private lazy val _spark = QuickSQLContextFactory.getOrCreate()
  val dataPath = s"${this.getClass.getResource("/data").getFile}"
  def getDatasetPath(datasetName: String): String = s"$dataPath/$datasetName"
  /**
    * Load dataset from /data folder in resources folder
    * @param datasetName
    * @return
    */
  def loadDataset(datasetName: String, idCol: String, targetTagCol: String, textCol: String): DataFrame = {
    _spark.read.option("delimiter", ")").csv(getDatasetPath(datasetName)).createOrReplaceGlobalTempView("temp")
    _spark.sql(s"""SELECT regexp_extract(_c0, '[(]([^,]*),(.*)', 1) AS $idCol,regexp_extract(_c0, '[(]([^,]*),(.*)', 2) AS $targetTagCol,_c1 AS $textCol FROM global_temp.temp""")}
}
