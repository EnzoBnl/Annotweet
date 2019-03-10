package com.enzobnl.annotweet.systems

case class TSACrossValidationResult(mean: Double, stddev: Double){
  override def toString: String = s"${mean.toFloat*100}% +- ${stddev.toFloat*100}%"
}
