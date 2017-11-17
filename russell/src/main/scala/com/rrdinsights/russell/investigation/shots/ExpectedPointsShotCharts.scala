package com.rrdinsights.russell.investigation.shots

import java.{lang => jl}

import com.rrdinsights.russell.commandline.{CommandLineBase, SeasonOption}
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{DataModelUtils, ShotWithPlayers}
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.TimeUtils

object ExpectedPointsShotCharts {

  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow
    val args = PlayerWithShotsArguments(strings)
    val where = args.season.map(v => Seq("season = '$v'")).getOrElse(Seq.empty)

    val shotsWithPlayers = readShotsWithPlayers(where: _*)
      .map(v => ((v.shooter, ShotZone.findShotZone(v.xCoordinate, v.yCoordinate, v.shotValue).toString), v))

    val shotCharts = readShotCharts()
      .map(v => ((v.playerId, v.bin), v))
      .toMap

    val scored = joinShotsWithShotCharts(shotsWithPlayers, shotCharts)
      .map(v => toScoredShot(v._1, v._2, dt))
      .groupBy(v => (v.defenseTeamId, v.bin))
      .map(v => (v._1, v._2.reduce(_ + _)))
      .map(v => v._2.copy(primaryKey = v._1.toString()))
      .toSeq

    //println(s"${scored.size} Records to write")
    scored.foreach(println)

    writeScoredShots(scored)
  }

  private def joinShotsWithShotCharts(shotsWithPlayers: Seq[((Integer, String), ShotWithPlayers)], shotCharts: Map[(Integer, String), PlayerShotChartSection]): Seq[(ShotWithPlayers, PlayerShotChartSection)] =
    shotsWithPlayers.map(v => (v._2, shotCharts.getOrElse(v._1, emptyShotChartSection(v._1._1, v._1._2))))

  private def emptyShotChartSection(playerId: Integer, bin: String): PlayerShotChartSection =
    PlayerShotChartSection(
      s"${playerId}_$bin",
      playerId,
      bin,
      ShotZone.find(bin).value,
      0,
      0,
      null)

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

      DataModelUtils.gameIdToSeason(shotWithPlayers.gameId),
      dt)
  }

}

final case class ScoredShot(primaryKey: String,
                            gameId: String,
                            eventNumber: jl.Integer,
                            shooter: jl.Integer,
                            offenseTeamId: jl.Integer,
                            offensePlayer1Id: jl.Integer,
                            offensePlayer2Id: jl.Integer,
                            offensePlayer3Id: jl.Integer,
                            offensePlayer4Id: jl.Integer,
                            offensePlayer5Id: jl.Integer,
                            defenseTeamId: jl.Integer,
                            defensePlayer1Id: jl.Integer,
                            defensePlayer2Id: jl.Integer,
                            defensePlayer3Id: jl.Integer,
                            defensePlayer4Id: jl.Integer,
                            defensePlayer5Id: jl.Integer,
                            bin: String,
                            shotValue: jl.Integer,
                            shotAttempted: jl.Integer,
                            shotMade: jl.Integer,
                            expectedPoints: jl.Double,
                            season: String,
                            dt: String) {

  def +(other: ScoredShot): ScoredShot = this.copy(
    shotAttempted = shotAttempted + other.shotAttempted,
    shotMade = shotMade + other.shotMade,
    expectedPoints = expectedPoints + other.expectedPoints)
}

private final class ExpectedPointsArguments private(args: Array[String])
  extends CommandLineBase(args, "Player Stats") with SeasonOption

private object ExpectedPointsArguments {

  def apply(args: Array[String]): ExpectedPointsArguments = new ExpectedPointsArguments(args)

}
