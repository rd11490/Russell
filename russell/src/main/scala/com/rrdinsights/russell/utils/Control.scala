package com.rrdinsights.russell.utils

/**
  * From David Pollak's Control Structure.
  */
private[russell] object Control {

  /**
    *
    * @param param of type A; passed to func
    * @param func any function or whatever that matches the signature
    * @tparam A any type with def close(): Unit; Java's Closeable interface should be compatible
    * @tparam B any type including Any
    * @return of type B
    */
  def using[A <: {def close() : Unit}, B](param: A)(func: A => B): B =
    try {
      func(param)
    } finally {
      // close even when an Exception is caught
      if (param != null) param.close()
    }

}