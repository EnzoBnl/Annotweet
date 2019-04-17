package com.enzobnl.annotweet.systems

/**
  * Tag class is used to manage tweets labels
  */
object Tag extends Enumeration {
  val Neg, Neu, Pos, Irr, Nothing = Value

  /**
    * Convert string to corresponding Tag
    * @param s
    * @return
    */
  def get(s: String): Tag.Value = {
    s match {
      case "Neg" => Tag.Neg
      case "Neu" => Tag.Neu
      case "Pos" => Tag.Pos
      case "Irr" => Tag.Irr
      case _ => Tag.Nothing
    }
  }
}
