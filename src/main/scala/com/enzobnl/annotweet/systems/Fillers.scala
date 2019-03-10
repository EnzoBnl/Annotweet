package com.enzobnl.annotweet.systems


case class Fillers(array: Array[String]){
  override def toString: String = array.foldLeft("(")((acc: String, s: String) => s"$acc,$s") + ")"
}
