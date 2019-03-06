package com.enzobnl.annotweet.system


object Tag extends Enumeration {
  val Neg, Neu, Pos, Irr, Nothing = Value
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
