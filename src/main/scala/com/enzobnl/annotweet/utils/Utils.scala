package com.enzobnl.annotweet.utils
import java.nio.file.{Files, Path, Paths, StandardCopyOption}



object Utils {


  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / Math.pow(10, 6) + "ms")
    result
  }

  /**
    *
    * @param block
    * @tparam R
    * @return elapsed time in ms
    */
  def getTime[R](block: => R): Double = {
    val t0 = System.nanoTime()
    block // call-by-name
    val t1 = System.nanoTime()
    (t1 - t0) / Math.pow(10, 6)
  }


  def moveAndRename(sourceFilename: String, destinationFilename: String): Unit = {
    val path = Files.move(
      Paths.get(sourceFilename),
      Paths.get(destinationFilename),
      StandardCopyOption.REPLACE_EXISTING
    )

    if (path != null) {
      println(s"moved the file $sourceFilename successfully")
    } else {
      println(s"could NOT move the file $sourceFilename")
    }
  }

  def deleteFolder(tmpFolder: String) = {
    Files.list(Paths.get(tmpFolder)).toArray.foreach((p: AnyRef) => Files.delete(p.asInstanceOf[Path]))
    Files.delete(Paths.get(tmpFolder))
  }

}
