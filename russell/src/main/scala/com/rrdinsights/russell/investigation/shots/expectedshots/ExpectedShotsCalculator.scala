package com.rrdinsights.russell.investigation.shots.expectedshots

import java.{lang => jl}

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{ExpectedPoints, ScoredShot}
import com.rrdinsights.russell.storage.tables.{MySqlTable, NBATables}
import com.rrdinsights.russell.utils.TimeUtils

object ExpectedShotsCalculator {

  import com.rrdinsights.russell.utils.MathUtils._

  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow
    val args = ExpectedPointsArguments(strings)
    val season = args.season
    val seasonType = args.seasonType

    val where = Seq(s"season = '$season'", s"seasonType = '$seasonType'")
    val scoredShots = readScoredShots(where)

    if (args.offense) {
      offenseZoned(scoredShots, dt, season, seasonType)
    }

    if (args.defense) {
      defenseZoned(scoredShots, dt, season, seasonType)
    }

  }

  private def reduceShotGroup(key: (Integer, String), shots: Seq[ExpectedPointsForReduction], dt: String, season: String, seasonType: String): ExpectedPoints = {
    val attempted = shots.map(v => v.shotAttempts.intValue()).sum
    val made = shots.map(v => v.shotMade.intValue()).sum
    val expectedPoints = shots.map(v => v.expectedPoints.doubleValue())
    val expectedPointsAvg = mean(expectedPoints)
    val expectedPointsStDev = stdDev(expectedPoints)
    val value = shots.head.shotValue
    val points = shots.map(v => v.shotMade * v.shotValue)
    val pointsAvg = mean(points)
    val pointsStDev = stdDev(points)

    ExpectedPoints(
      s"${key._1}_${key._2}_${season}_$seasonType",
      key._1,
      key._2,
      attempted,
      made,
      value,
      pointsAvg,
      pointsStDev,
      expectedPointsAvg,
      expectedPointsStDev,
      season,
      seasonType,
      dt)
  }

  private def reduceShots(shots: Seq[ExpectedPointsForReduction], dt: String, season: String, seasonType: String): Seq[ExpectedPoints] =
    shots
      .groupBy(v => (v.teamId, v.bin))
      .map(v => reduceShotGroup(v._1, v._2, dt, season, seasonType))
      .toSeq

  private def offenseZoned(scoredShot: Seq[ScoredShot], dt: String, season: String, seasonType: String): Unit = {
    val shotsForReduction = scoredShot.flatMap(v => Seq(
      ExpectedPointsForReduction(
        v.offenseTeamId,
        v.bin,
        v.shotAttempted,
        v.shotMade,
        v.shotValue,
        v.expectedPoints),
      ExpectedPointsForReduction(
        v.offenseTeamId,
        "Total",
        v.shotAttempted,
        v.shotMade,
        v.shotValue,
        v.expectedPoints),
      ExpectedPointsForReduction(
        0,
        v.bin,
        v.shotAttempted,
        v.shotMade,
        v.shotValue,
        v.expectedPoints),
      ExpectedPointsForReduction(
        0,
        "Total",
        v.shotAttempted,
        v.shotMade,
        v.shotValue,
        v.expectedPoints)))

    val shots = reduceShots(shotsForReduction, dt, season, seasonType)

    writeShots(NBATables.offense_expected_points, shots)
  }

  private def defenseZoned(scoredShot: Seq[ScoredShot], dt: String, season: String, seasonType: String): Unit = {
    val shotsForReduction = scoredShot.flatMap(v =>
      Seq(
        ExpectedPointsForReduction(
          v.defenseTeamId,
          v.bin,
          v.shotAttempted,
          v.shotMade,
          v.shotValue,
          v.expectedPoints),
        ExpectedPointsForReduction(
          v.defenseTeamId,
          "Total",
          v.shotAttempted,
          v.shotMade,
          v.shotValue,
          v.expectedPoints),
        ExpectedPointsForReduction(
          0,
          v.bin,
          v.shotAttempted,
          v.shotMade,
          v.shotValue,
          v.expectedPoints),
        ExpectedPointsForReduction(
          0,
          "Total",
          v.shotAttempted,
          v.shotMade,
          v.shotValue,
          v.expectedPoints)))

    val shots = reduceShots(shotsForReduction, dt, season, seasonType)

    writeShots(NBATables.defense_expected_points, shots)
  }

  def writeShots(table: MySqlTable, shots: Seq[ExpectedPoints]): Unit = {
    MySqlClient.createTable(table)
    MySqlClient.insertInto(table, shots)
  }

  private def readScoredShots(where: Seq[String]): Seq[ScoredShot] =
    MySqlClient.selectFrom(NBATables.team_scored_shots, ScoredShot.apply, where: _*)
}

final case class ExpectedPointsForReduction(
                                             teamId: Integer,
                                             bin: String,
                                             shotAttempts: Integer,
                                             shotMade: Integer,
                                             shotValue: Integer,
                                             expectedPoints: jl.Double)