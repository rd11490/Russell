package com.rrdinsights.russell.investigation.shots

import com.rrdinsights.russell.etl.application.ShotChartDownloader

object ShotChartExplore {
  /**
    * This object is just for playing with play by play data to help with production tasks
    *
    */
  def main(strings: Array[String]): Unit = {
    val shots = ShotChartDownloader.readShotData("playerId = '1503'")
    val histo = ShotHistogram.calculate(shots)
    histo.toSeq.sortBy(v => v._2.shots).reverse.foreach(println)

  }


}