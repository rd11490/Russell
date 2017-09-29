package com.rrdinsights.russell.spark.job

sealed trait ExitStatus {
  def status: Int
}

object ExitStatus {
  case object Success extends ExitStatus {
    override def status: Int = 0
  }
  case object Failed extends ExitStatus {
    override def status: Int = 1
  }
  case object UnhandledException extends ExitStatus {
    override def status: Int = 255
  }
}

class ExitStatusException(msg: String) extends Exception(msg)

object ExitStatusException {
  def apply(status: Int): ExitStatusException =
    new ExitStatusException(s"Job exited with Status Code: $status")
}

