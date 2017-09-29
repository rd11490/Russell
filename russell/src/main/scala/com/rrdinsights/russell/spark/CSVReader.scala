package com.rrdinsights.russell.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object CSVReader {

  def readCSV(path: String, options: Map[String, String] = Map.empty)(implicit spark: SparkSession): DataFrame =
    spark.read.options(options).csv(path)
}
