package com.rrdinsights.russell.investigation.shots.expectedshots

import com.rrdinsights.russell.commandline.{CommandLineBase, SeasonOption}
import com.rrdinsights.russell.investigation.shots.{PlayerShotChartSection, ShotZone}
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{DataModelUtils, ScoredShot, ShotWithPlayers}
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.TimeUtils

object ScoreShotsCalculator {

  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow
    val args = ScoredShotsArguments(strings)
    val where = args.seasonOpt.map(v => Seq(s"season = '$v'")).getOrElse(Seq.empty)

    val shotsWithPlayers = readShotsWithPlayers(where: _*)
      .map(v => ((v.shooter, ShotZone.findShotZone(v.xCoordinate, v.yCoordinate, v.shotValue).toString), v))

    val shotCharts = readShotCharts()
      .map(v => ((v.playerId, v.bin), v))
      .toMap

    val scored = joinShotsWithShotCharts(shotsWithPlayers, shotCharts)
      .map(v => toScoredShot(v._1, v._2, dt))

    writeScoredShots(scored)
  }

  private def joinShotsWithShotCharts(shotsWithPlayers: Seq[((Integer, String), ShotWithPlayers)], shotCharts: Map[(Integer, String), PlayerShotChartSection]): Seq[(ShotWithPlayers, PlayerShotChartSection)] =
    shotsWithPlayers.map(v => (v._2, shotCharts.getOrElse(v._1, emptyShotChartSection(v._1._1, v._1._2))))

  private def emptyShotChartSection(playerId: Integer, bin: String): PlayerShotChartSection = {
    println(s"$playerId - $bin")
    PlayerShotChartSection(
      s"${playerId}_$bin",
      playerId,
      bin,
      ShotZone.find(bin).value,
      0,
      0,
      null)
  }

  private def readShotsWithPlayers(where: String*): Seq[ShotWithPlayers] =
    MySqlClient.selectFrom(NBATables.lineup_shots, ShotWithPlayers.apply, where: _*)

  private def readShotCharts(/*IO*/): Seq[PlayerShotChartSection] =
    MySqlClient.selectFrom(NBATables.player_shot_charts, PlayerShotChartSection.apply)

  private def writeScoredShots(shots: Seq[ScoredShot]): Unit = {
    MySqlClient.createTable(NBATables.team_scored_shots)
    MySqlClient.insertInto(NBATables.team_scored_shots, shots)
  }

  private def toScoredShot(shotWithPlayers: ShotWithPlayers, shotChart: PlayerShotChartSection, dt: String): ScoredShot = {
    ScoredShot(
      s"${shotWithPlayers.gameId}_${shotWithPlayers.eventNumber}",
      shotWithPlayers.gameId,
      shotWithPlayers.eventNumber,
      shotWithPlayers.shooter,

      shotWithPlayers.offenseTeamId,
      shotWithPlayers.offensePlayer1Id,
      shotWithPlayers.offensePlayer2Id,
      shotWithPlayers.offensePlayer3Id,
      shotWithPlayers.offensePlayer4Id,
      shotWithPlayers.offensePlayer5Id,

      shotWithPlayers.defenseTeamId,
      shotWithPlayers.defensePlayer1Id,
      shotWithPlayers.defensePlayer2Id,
      shotWithPlayers.defensePlayer3Id,
      shotWithPlayers.defensePlayer4Id,
      shotWithPlayers.defensePlayer5Id,

      shotChart.bin,
      shotChart.value,

      shotWithPlayers.shotAttemptedFlag,
      shotWithPlayers.shotMadeFlag,

      (shotChart.made.toDouble / shotChart.shots.toDouble) * shotChart.value,
      shotChart.shots,
      shotChart.made,

      DataModelUtils.gameIdToSeason(shotWithPlayers.gameId),
      shotWithPlayers.seasonType,
      dt)
  }

}



private final class ScoredShotsArguments private(args: Array[String])
  extends CommandLineBase(args, "Player Stats") with SeasonOption

private object ScoredShotsArguments {

  def apply(args: Array[String]): ScoredShotsArguments = new ScoredShotsArguments(args)

}