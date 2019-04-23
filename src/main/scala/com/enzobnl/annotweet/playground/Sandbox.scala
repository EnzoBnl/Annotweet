package com.enzobnl.annotweet.playground

import com.enzobnl.annotweet.resourcesmanaging.TweetDatasetsManager
import com.enzobnl.annotweet.utils.QuickSQLContextFactory
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.param.shared.HasRegParam
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
class C(override val uid: String) extends Transformer with HasRegParam{
  new StringIndexer
  final val i = new Param[String](this, "i", "it's i")

  def setI(v: String): this.type = set(i, v)
  override def transform(dataset: Dataset[_]): DataFrame = ???

  override def copy(extra: ParamMap): Transformer = ???

  override def transformSchema(schema: StructType): StructType = ???
}
class D(override val uid: String) extends Transformer with HasRegParam{
  new StringIndexer
  final val i = new Param[String](this, "i", "it's i")

  def setI(v: String): this.type = set(i, v)
  override def transform(dataset: Dataset[_]): DataFrame = ???

  override def copy(extra: ParamMap): Transformer = ???

  override def transformSchema(schema: StructType): StructType = ???
}
object Sandbox extends App{
  val c = new C("koegpkoepko")
  val d = new D("kkerko")
  c.set(c.regParam, 1.0 )
  d.set(d.regParam , 2.0 )
  println(c.getOrDefault(c.regParam))
  println(d.getOrDefault(d.regParam))

  //    val spark = QuickSQLContextFactory.getOrCreate()
//      import spark.implicits._
//      val ds = TweetDatasetsManager.loadDataset("air.txt", "id", "target", "text")
//      val sorter = udf((t: (String, Int)) => t._2)
//      val ds_ = ds.flatMap({ t: Row => t.getAs[String]("text").split(" ")}).map({ word: String => (word, 1)}).groupByKey(_._1.toLowerCase()).reduceGroups { (p1: (String, Int), p2: (String, Int)) => {(p1._1, p1._2 + p2._2)}}.toDF("1", "2")
//        ds_.orderBy(col(ds_.schema.fields(0).name)).select(col("1"), ds_.apply("1")).show(5)

}
