package com.enzobnl.annotweet.utils

object Utils {
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / Math.pow(10, 6) + "ms")
    result
  }
  def getTime[R](block: => R): Double = {
    val t0 = System.nanoTime()
    block // call-by-name
    val t1 = System.nanoTime()
    (t1 - t0) / Math.pow(10, 6)
  }
}
