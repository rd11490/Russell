package com.rrdinsights.russell.investigation.shots

import java.{lang => jl}

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{RawShotData, ScoredShot, ShotWithPlayers}
import com.rrdinsights.russell.storage.tables.NBATables


object ShotUtils {

  def buildShotChart(shots: Seq[RawShotData], dt: String): Seq[PlayerShotChartSection] = {
    shots
      .groupBy(_.playerId)
      .flatMap(v => ShotHistogram.calculate(v._2))
      .map(v => PlayerShotChartSection.apply(v._1, v._2, dt))
      .toSeq
  }

  def buildShotChartTotal(shots: Seq[RawShotData], dt: String): Seq[ShotChartTotalSection] = {
    shots.groupBy(_.playerId)
      .map(v => (v._1, calculatePointsPerShot(v._2)))
      .map(v => ShotChartTotalSection(v._1, v._2.attempts, v._2.made, v._2.pointsPerShot))
      .toSeq
  }


  def calculatePointsPerShot(shots: Seq[RawShotData]): ShotChartTotalInfo = {
    val made = shots.count(_.shotMadeFlag.intValue() == 1)
    val attempts = shots.size
    val pointsPerShot = shots.map(v => v.shotMadeFlag.intValue() * v.shotValue.doubleValue()).sum / shots.size

    ShotChartTotalInfo(made, attempts, pointsPerShot)
  }

  def readShotsWithPlayers(where: String*): Seq[ShotWithPlayers] =
    MySqlClient.selectFrom(NBATables.lineup_shots, ShotWithPlayers.apply, where: _*)

  def readShots(where: String*): Seq[RawShotData] =
    MySqlClient.selectFrom(NBATables.raw_shot_data, RawShotData.apply, where: _*)

  def readScoredShots(where: String*): Seq[ScoredShot] =
    MySqlClient.selectFrom(NBATables.team_scored_shots, ScoredShot.apply, where: _*)

}

final case class ShotChartTotalInfo(made: jl.Integer, attempts: jl.Integer, pointsPerShot: jl.Double)

final case class ShotChartTotalSection(playerId: Integer, made: jl.Integer, attempts: jl.Integer, pointsPerShot: jl.Double)

final case class ShotStintByZoneData(
                                      primaryKey: String,
                                      offensePlayer1Id: jl.Integer,
                                      offensePlayer2Id: jl.Integer,
                                      offensePlayer3Id: jl.Integer,
                                      offensePlayer4Id: jl.Integer,
                                      offensePlayer5Id: jl.Integer,
                                      offenseTeamId: jl.Integer,
                                      defensePlayer1Id: jl.Integer,
                                      defensePlayer2Id: jl.Integer,
                                      defensePlayer3Id: jl.Integer,
                                      defensePlayer4Id: jl.Integer,
                                      defensePlayer5Id: jl.Integer,
                                      defenseTeamId: jl.Integer,
                                      bin: String,
                                      attempts: jl.Integer,
                                      made: jl.Integer,
                                      shotPoints: jl.Double,
                                      shotExpectedPoints: jl.Double,
                                      playerExpectedPoints: jl.Double,
                                      difference: jl.Double,
                                      season: String) {
  def +(other: ShotStintByZoneData): ShotStintByZoneData =
    ShotStintByZoneData(
      primaryKey,
      offensePlayer1Id,
      offensePlayer2Id,
      offensePlayer3Id,
      offensePlayer4Id,
      offensePlayer5Id,
      offenseTeamId,
      defensePlayer1Id,
      defensePlayer2Id,
      defensePlayer3Id,
      defensePlayer4Id,
      defensePlayer5Id,
      defenseTeamId,
      bin,
      attempts + other.attempts,
      made + other.made,
      shotPoints + other.shotPoints,
      shotExpectedPoints + other.shotExpectedPoints,
      playerExpectedPoints + other.playerExpectedPoints,
      difference + other.difference,
      season)

  def /(scalar: Double): ShotStintByZoneData =
    ShotStintByZoneData(
      primaryKey,
      offensePlayer1Id,
      offensePlayer2Id,
      offensePlayer3Id,
      offensePlayer4Id,
      offensePlayer5Id,
      offenseTeamId,
      defensePlayer1Id,
      defensePlayer2Id,
      defensePlayer3Id,
      defensePlayer4Id,
      defensePlayer5Id,
      defenseTeamId,
      bin,
      attempts,
      made,
      shotPoints / scalar,
      shotExpectedPoints / scalar,
      playerExpectedPoints / scalar,
      difference / scalar,
      season)
}

final case class FullScoredShot(
                                 primaryKey: String,
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
                                 playerShotAttempted: jl.Integer,
                                 playerShotMade: jl.Integer,
                                 playerTotalExpectedPoints: jl.Double,
                                 difference: jl.Double,
                                 season: String,
                                 dt: String)