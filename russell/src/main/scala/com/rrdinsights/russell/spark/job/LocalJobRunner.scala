package com.rrdinsights.russell.spark.job

import org.apache.spark.SparkConf

trait LocalJobRunner extends JobRunner {

  def main(args: Array[String]): Unit = {
    runSpark2(args)(spark)
  }

  override val sparkConf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("test")
    .set("spark.driver.host", "localhost")
    .set("spark.ui.enabled", "false")
    .set("spark.app.id", "app")
    .set("spark.default.parallelism", "1")
    .set("spark.shuffle.compress", "false")
    .set("spark.broadcast.compress", "false")
    .set("spark.rdd.compress", "false")
}