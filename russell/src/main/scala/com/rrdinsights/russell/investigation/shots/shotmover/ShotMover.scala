package com.rrdinsights.russell.investigation.shots.shotmover

import com.rrdinsights.russell.investigation.shots.{PlayerShotChartSection, ShotHistogram, ShotZone}
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{RawShotData, ShotWithPlayers}
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.{MapJoin, TimeUtils}
import java.{lang => jl}

import com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedPointsArguments

object ShotMover {

  def main(strings: Array[String]): Unit = {

    val dt = TimeUtils.dtNow
    val args = ExpectedPointsArguments(strings)
    val season = args.season


    val whereSeason = s"season = '$season'"
    val whereBeforeSeason = s"season <= '$season'" // Use shots from the current season - not best practice, but don't have college shot charts for rookies

    val shots = readShots(whereBeforeSeason)

    val shotChartSections = buildShotChart(shots, dt)
      .map(v => ((v.playerId, v.bin), v))
      .toMap

    println("shotChartSections")
    println(shotChartSections.size)

    val shotChartTotalSection = buildShotChartTotal(shots, dt)
      .map(v => (v.playerId, v))
      .toMap

    println("shotChartTotalSection")
    println(shotChartTotalSection.size)

    val shotsWithPlayers = readShotsWithPlayers(whereSeason)

    val mappedshotsWithPlayers = shotsWithPlayers
      .map(v => ((v.shooter, ShotZone.findShotZone(v.xCoordinate, v.yCoordinate, v.shotValue).toString), v))

    val scoredShotSection = MapJoin.joinSeq(mappedshotsWithPlayers, shotChartSections)
      .map(v => (v._1.shooter, v))

    val fullScoredShots = scoredShotSection.map(v => (v._2, buildPointsPerShotLineup(v._2._1, shotChartTotalSection)))
      .map(v => toFullScoredShot(v._1._1, v._1._2, v._2, season, dt))

    val stints = reduceStints(fullScoredShots, season)

    writeShotStints(stints)
  }

  private def buildPointsPerShotLineup(shot: ShotWithPlayers, shotMap: Map[Integer, ShotChartTotalSection]): ShotChartTotalSection = {
    val attemptsAndPoitns = Seq(
      extractShotValue(shot.offensePlayer1Id, shotMap),
      extractShotValue(shot.offensePlayer2Id, shotMap),
      extractShotValue(shot.offensePlayer3Id, shotMap),
      extractShotValue(shot.offensePlayer4Id, shotMap),
      extractShotValue(shot.offensePlayer5Id, shotMap))
      .flatten
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))

    val pointsPerShotLineup = attemptsAndPoitns._3/attemptsAndPoitns._1.doubleValue()

    ShotChartTotalSection(shot.shooter, attemptsAndPoitns._2, attemptsAndPoitns._1, pointsPerShotLineup)
  }

  private def extractShotValue(id: Integer, shotMap: Map[Integer, ShotChartTotalSection]): Option[(Int, Int, Double)] =
    shotMap.get(id).map(v => (v.attempts, v.made, v.attempts*v.pointsPerShot))

  private def writeShotStints(stints: Seq[ShotStintData]): Unit = {
    MySqlClient.createTable(NBATables.shot_stint_data)
    MySqlClient.insertInto(NBATables.shot_stint_data, stints)
  }

  private def reduceStints(shots: Seq[FullScoredShot], season: String): Seq[ShotStintData] = {
    shots.groupBy(v => (
      v.offensePlayer1Id,
      v.offensePlayer2Id,
      v.offensePlayer3Id,
      v.offensePlayer4Id,
      v.offensePlayer5Id,
      v.defensePlayer1Id,
      v.defensePlayer2Id,
      v.defensePlayer3Id,
      v.defensePlayer4Id,
      v.defensePlayer5Id))
      .map(v => v._2.map(v => ShotStintData(
        Seq(v.offensePlayer1Id,
          v.offensePlayer2Id,
          v.offensePlayer3Id,
          v.offensePlayer4Id,
          v.offensePlayer5Id,
          v.defensePlayer1Id,
          v.defensePlayer2Id,
          v.defensePlayer3Id,
          v.defensePlayer4Id,
          v.defensePlayer5Id).mkString("_"),
        v.offensePlayer1Id,
        v.offensePlayer2Id,
        v.offensePlayer3Id,
        v.offensePlayer4Id,
        v.offensePlayer5Id,
        v.defensePlayer1Id,
        v.defensePlayer2Id,
        v.defensePlayer3Id,
        v.defensePlayer4Id,
        v.defensePlayer5Id,
        1,
        v.shotMade,
        v.shotValue.toDouble * v.shotMade,
        v.expectedPoints,
        v.playerTotalExpectedPoints,
        v.difference,
        season))
        .reduce(_ + _) / v._2.size.doubleValue())
      .toSeq
  }

  private def toFullScoredShot(shotWithPlayers: ShotWithPlayers, shotChart: PlayerShotChartSection, playerExpectedPointsPerShot: ShotChartTotalSection, season: String, dt: String): FullScoredShot = {
    val expectedPoints = (shotChart.made.toDouble / shotChart.shots.toDouble) * shotChart.value

    FullScoredShot(
      s"${shotWithPlayers.gameId}_${shotWithPlayers.eventNumber}_$season",
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

      expectedPoints,
      shotChart.shots,
      shotChart.made,

      playerExpectedPointsPerShot.pointsPerShot,

      expectedPoints - playerExpectedPointsPerShot.pointsPerShot,

      season,
      dt)
  }


  private def readShotsWithPlayers(where: String*): Seq[ShotWithPlayers] =
    MySqlClient.selectFrom(NBATables.lineup_shots, ShotWithPlayers.apply, where: _*)

  private def readShots(where: String*): Seq[RawShotData] =
    MySqlClient.selectFrom(NBATables.raw_shot_data, RawShotData.apply, where: _*)

  private def buildShotChart(shots: Seq[RawShotData], dt: String): Seq[PlayerShotChartSection] = {
    shots
      .groupBy(_.playerId)
      .flatMap(v => ShotHistogram.calculate(v._2))
      .map(v => PlayerShotChartSection.apply(v._1, v._2, dt))
      .toSeq
  }

  private def buildShotChartTotal(shots: Seq[RawShotData], dt: String): Seq[ShotChartTotalSection] = {
    shots.groupBy(_.playerId)
      .map(v => (v._1, calculatePointsPerShot(v._2)))
      .map(v => ShotChartTotalSection(v._1, v._2.attempts, v._2.made, v._2.pointsPerShot))
      .toSeq
  }


  private def calculatePointsPerShot(shots: Seq[RawShotData]): ShotChartTotalInfo = {
    val made = shots.count(_.shotMadeFlag.intValue() == 1)
    val attempts = shots.size
    val pointsPerShot = shots.map(v => v.shotMadeFlag.intValue() * v.shotValue.doubleValue()).sum / shots.size

    ShotChartTotalInfo(made, attempts, pointsPerShot)
  }


}

private final case class ShotChartTotalInfo(made: jl.Integer, attempts: jl.Integer, pointsPerShot: jl.Double)

private final case class ShotChartTotalSection(playerId: Integer, made: jl.Integer, attempts: jl.Integer, pointsPerShot: jl.Double)

final case class ShotStintData(
                                primaryKey: String,
                                offensePlayer1Id: jl.Integer,
                                offensePlayer2Id: jl.Integer,
                                offensePlayer3Id: jl.Integer,
                                offensePlayer4Id: jl.Integer,
                                offensePlayer5Id: jl.Integer,
                                defensePlayer1Id: jl.Integer,
                                defensePlayer2Id: jl.Integer,
                                defensePlayer3Id: jl.Integer,
                                defensePlayer4Id: jl.Integer,
                                defensePlayer5Id: jl.Integer,
                                attempts: jl.Integer,
                                made: jl.Integer,
                                shotPoints: jl.Double,
                                shotExpectedPoints: jl.Double,
                                playerExpectedPoints: jl.Double,
                                difference: jl.Double,
                                season: String) {
  def +(other: ShotStintData): ShotStintData =
    ShotStintData(
      primaryKey,
      offensePlayer1Id,
      offensePlayer2Id,
      offensePlayer3Id,
      offensePlayer4Id,
      offensePlayer5Id,
      defensePlayer1Id,
      defensePlayer2Id,
      defensePlayer3Id,
      defensePlayer4Id,
      defensePlayer5Id,
      attempts + other.attempts,
      made + other.made,
      shotPoints + other.shotPoints,
      shotExpectedPoints + other.shotExpectedPoints,
      playerExpectedPoints + other.playerExpectedPoints,
      difference + other.difference,
      season)

  def /(scalar: Double): ShotStintData =
    ShotStintData(
      primaryKey,
      offensePlayer1Id,
      offensePlayer2Id,
      offensePlayer3Id,
      offensePlayer4Id,
      offensePlayer5Id,
      defensePlayer1Id,
      defensePlayer2Id,
      defensePlayer3Id,
      defensePlayer4Id,
      defensePlayer5Id,
      attempts,
      made,
      shotPoints / scalar,
      shotExpectedPoints / scalar,
      playerExpectedPoints / scalar,
      difference / scalar,
      season)
}

private final case class FullScoredShot(
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