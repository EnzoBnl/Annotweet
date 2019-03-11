package com.enzobnl.annotweet.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


object QuickSQLContextFactory{
  private var _spark: Option[SQLContext] = None

  /**
    * Create a sparkSQLContext parametrized with appName and logLevel.
    * If already called during the runtime of the process, this will return the instance previously created
    * @param appName: Name the spark app
    * @param logLevel: "INFO", "WARN", "ERROR", "FATAL"
    * @return
    */
  def getOrCreate(appName: String="default", logLevel: String="ERROR"): SQLContext = {
    if(_spark.isEmpty){
      val sc = new SparkContext(new SparkConf().setAppName(appName).setMaster("local[*]").set("spark.driver.memory", "3g"))
      sc.setLogLevel(logLevel)
      _spark = Some(new SQLContext(sc))
    }
    _spark.get
  }
}
