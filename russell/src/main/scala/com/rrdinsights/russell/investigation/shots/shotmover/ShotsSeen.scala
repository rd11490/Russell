package com.rrdinsights.russell.investigation.shots.shotmover

import com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedPointsArguments
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.ShotWithPlayers
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.TimeUtils

object ShotsSeen {

  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow
    val args = ExpectedPointsArguments(strings)
    val season = args.season

    val whereSeason = s"season = '$season'"

    val shotsWithPlayers = readShotsWithPlayers(whereSeason)

    val shotsSeen = calculateShotsSeen(shotsWithPlayers, season)

    writeShotsSeen(shotsSeen)
  }

  private def writeShotsSeen(shotsSeen: Iterable[ShotsSeen]): Unit = {
    MySqlClient.createTable(NBATables.shots_seen)
    MySqlClient.insertInto(NBATables.shots_seen, shotsSeen.toSeq)
  }

  private def readShotsWithPlayers(where: String*): Seq[ShotWithPlayers] =
    MySqlClient.selectFrom(NBATables.lineup_shots, ShotWithPlayers.apply, where: _*)

  private def calculateShotsSeen(shotsWithPlayers: Seq[ShotWithPlayers], season: String): Iterable[ShotsSeen] = {
    shotsWithPlayers.flatMap(v => {
      Seq(
        (v.offensePlayer1Id, 1),
        (v.offensePlayer2Id, 1),
        (v.offensePlayer3Id, 1),
        (v.offensePlayer4Id, 1),
        (v.offensePlayer5Id, 1),
        (v.defensePlayer1Id, 1),
        (v.defensePlayer2Id, 1),
        (v.defensePlayer3Id, 1),
        (v.defensePlayer4Id, 1),
        (v.defensePlayer5Id, 1))
    })
      .groupBy(_._1)
      .map(v => ShotsSeen(s"${v._1}_$season", v._1, v._2.map(c => c._2).sum, season))
  }
}

final case class ShotsSeen(primaryKey: String, playerId: Integer, shots: Integer, season: String)