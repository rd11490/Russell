package com.rrdinsights.russell.spark.job

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait JobRunner {

  protected def sparkConf: SparkConf

  protected def spark: SparkSession = JobRunner.newSparkSession(sparkConf)

  def runSpark2(strings: Array[String])(implicit spark: SparkSession): Unit

}

object JobRunner {

  private def newSparkSession(conf: SparkConf): SparkSession = {
    val builder = SparkSession.builder().master("local")
      .config(conf)
    val session = builder.getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    session
  }

  def validateExitCode(exitCode: Int): Unit = {
    if (exitCode != 0) {
      throw ExitStatusException(exitCode)
    }
  }

}