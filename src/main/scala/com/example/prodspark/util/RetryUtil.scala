package com.example.prodspark.util

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object RetryUtil {

  @tailrec
  def retry[T](tries: Int)(fn: => T): T = {
    require(tries > 0)
    Try { fn } match {
      case Success(x) => x
      case _ if tries > 1 => retry(tries-1)(fn)
      case Failure(e) => throw e
    }
  }
}
