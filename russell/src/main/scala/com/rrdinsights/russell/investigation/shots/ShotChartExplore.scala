package com.rrdinsights.russell.investigation.shots

import com.rrdinsights.russell.commandline.SeasonArgs
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

    val args = new SeasonArgs(strings)
    val seasonWhere = s"season = '${args.season}'"
    val shotData = MySqlClient.selectFrom(NBATables.defense_expected_points,
                                          ExpectedPoints.apply,
                                          seasonWhere,
                                          "bin != 'Total'")

    val fileName = s"ThreePtD${args.season.replace("-","")}.csv"

    CSVWriter
      .CSVWriter(
        shotData
          .filter(_.value == 3)
          .map(v =>
            (TeamMapper.teamInfo(v.teamId, args.season).get.teamName,
             mapBin(v.bin),
             v.attempts.intValue()))
          .groupBy(v => (v._1, v._2))
          .map(v => (v._1, v._2.map(_._3).sum))
          .groupBy(_._1._1)
          .map(v => toTeam3Data(v._1, v._2))
          .toSeq
          .sortBy(_.total3s))
      .write(fileName)

  }

  private def toTeam3Data(teamId: String,
                          threeData: Map[(String, String), Int]): Team3Data = {
    val corner = threeData((teamId, "Corner"))
    val break = threeData((teamId, "AboveTheBreak"))
    val long3 = threeData((teamId, "27+"))

    val total = corner + break + long3
    Team3Data(
      teamId,
      corner,
      corner.doubleValue() / total.doubleValue(),
      break,
      break.doubleValue() / total.doubleValue(),
      long3,
      long3.doubleValue() / total.doubleValue(),
      total
    )
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

private final case class Team3Data(teamId: String,
                                   corner3s: Int,
                                   cornerPercent: Double,
                                   aboveTheBreak3s: Int,
                                   aboveTheBreakPercent: Double,
                                   long3s: Int,
                                   longPercent: Double,
                                   total3s: Int) {
  override def toString: String =
    s"$teamId - Total3s: $total3s - Corner3s: $corner3s - AboveTheBreak3s: $aboveTheBreak3s - Long3s: $long3s"
}
