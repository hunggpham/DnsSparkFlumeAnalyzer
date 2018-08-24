package com.madison.sparkstreaming

/**
 * Created by hpham on 12/15/15.
 */
  /**
   * An extractor object for parsing strings into integers.
   */
  object IntParam {
    def unapply(str: String): Option[Int] = {
      try {
        Some(str.toInt)
      } catch {
        case e: NumberFormatException => None
      }
    }
  }
