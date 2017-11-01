package com.rrdinsights.russell.investigation

import com.rrdinsights.russell.etl.application.{PlayByPlayDownloader, PlayersOnCourtDownloader, ShotChartDownloader}
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.tables.NBATables

object ShotChartExplore {
  /**
    * This object is just for playing with play by play data to help with production tasks
    *
    */
  def main(strings: Array[String]): Unit = {
    val shots = ShotChartDownloader.readShotData("playerId = '202686'")
    val histo = ShotHistogram.calculate(shots)
    histo.toSeq.sortBy(v => v._2.shots).reverse.foreach(println)

  }


}