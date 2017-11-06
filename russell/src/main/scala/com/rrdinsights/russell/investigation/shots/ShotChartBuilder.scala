package com.rrdinsights.russell.investigation.shots

import com.rrdinsights.russell.etl.application.ShotChartDownloader
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.TimeUtils

object ShotChartBuilder {

  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow

    val shots = ShotChartDownloader.readShotData()

    val shotHistograms = shots
      .groupBy(_.playerId)
      .flatMap(v => ShotHistogram.calculate(v._2))
      .map(v => PlayerShotChartSection.apply(v._1, v._2, dt))
      .toSeq

    writeShotCharts(shotHistograms)
  }

  def writeShotCharts(shots: Seq[PlayerShotChartSection]): Unit = {
    MySqlClient.createTable(NBATables.player_shot_charts)
    MySqlClient.insertInto(NBATables.player_shot_charts, shots)
  }

}
