package com.rrdinsights.russell.utils

object NullSafe {

  def isNullOrEmpty(s: String): Boolean = s != null && s.nonEmpty

  def isNullOrEmpty[T <: Traversable[_]](s: T): Boolean = s != null && s.nonEmpty

}
