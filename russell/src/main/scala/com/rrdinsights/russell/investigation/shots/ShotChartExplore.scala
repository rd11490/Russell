package com.rrdinsights.russell.investigation.shots

import com.rrdinsights.russell.etl.application.ShotChartDownloader
import com.rrdinsights.russell.investigation.TeamMapper
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.ExpectedPoints
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.scalabrine.utils.CSVWriter

object ShotChartExplore {
  /**
    * This object is just for playing with shot data to help with production tasks
    *
    */
  def main(strings: Array[String]): Unit = {
    val shotData = MySqlClient.selectFrom(NBATables.defense_expected_points_zoned, ExpectedPoints.apply, "season = '2017-18'")

    CSVWriter.CSVWriter(
    shotData
      .filter(_.value == 3)
      .map(v => (TeamMapper.teamInfo(v.teamId, "2017-18").get.teamName, mapBin(v.bin), v.attempts.intValue()))
      .groupBy(v => (v._1, v._2))
      .map(v => (v._1, v._2.map(_._3).sum))
      .groupBy(_._1._1)
      .map(v => toTeam3Data(v._1, v._2))
      .toSeq
      .sortBy(_.total3s))
      .write("ThreePtD.csv")

  }

  private def toTeam3Data(teamId: String, threeData: Map[(String, String), Int]): Team3Data = {
    val corner = threeData((teamId, "Corner"))
    val break = threeData((teamId, "AboveTheBreak"))
    val long3 = threeData((teamId, "27+"))
    Team3Data(
      teamId,
      corner,
      break,
      long3,
      corner + break + long3)
  }

  private def mapBin(str: String): String = {
    if (str.contains("Long")) {
      "27+"
    } else if (str.contains("Corner")) {
      "Corner"
    } else {
      "AboveTheBreak"
    }
  }


}

private final case class Team3Data(teamId: String, corner3s: Int, aboveTheBreak3s: Int, long3s: Int, total3s: Int) {
  override def toString: String =
    s"$teamId - Total3s: $total3s - Corner3s: $corner3s - AboveTheBreak3s: $aboveTheBreak3s - Long3s: $long3s"
}