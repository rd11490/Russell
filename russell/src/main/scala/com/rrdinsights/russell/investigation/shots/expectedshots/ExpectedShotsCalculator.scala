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
    val season = args.seasonOpt.getOrElse(throw new IllegalArgumentException("Must provide a season"))
    val where = Seq(s"season = '$season'")
    val scoredShots = readScoredShots(where)

    if (args.offense) {
      offenseTotal(scoredShots, dt, season)
      if (args.zoned) {
        offenseZoned(scoredShots, dt, season)
      }
    }

    if (args.defense) {
      defenseTotal(scoredShots, dt, season)
      if (args.zoned) {
        defenseZoned(scoredShots, dt, season)
      }
    }

  }

  private def reduceShotGroup(key: (Integer, String), shots: Seq[ExpectedPointsForReduction], dt: String, season: String): ExpectedPoints = {
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
      s"${key._1}_${key._2}_$season",
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
      dt)
  }

  private def reduceShots(shots: Seq[ExpectedPointsForReduction], dt: String, season: String): Seq[ExpectedPoints] =
    shots
      .groupBy(v => (v.teamId, v.bin))
      .map(v => reduceShotGroup(v._1, v._2, dt, season))
      .toSeq


  private def offenseTotal(scoredShot: Seq[ScoredShot], dt: String, season: String): Unit = {
    val shotsForReduction = scoredShot.map(v => ExpectedPointsForReduction(
      v.offenseTeamId,
      "Total",
      v.shotAttempted,
      v.shotMade,
      v.shotValue,
      v.expectedPoints))

    val shots = reduceShots(shotsForReduction, dt, season)

    writeShots(NBATables.offense_expected_points_total, shots)
  }

  private def offenseZoned(scoredShot: Seq[ScoredShot], dt: String, season: String): Unit = {
    val shotsForReduction = scoredShot.map(v => ExpectedPointsForReduction(
      v.offenseTeamId,
      v.bin,
      v.shotAttempted,
      v.shotMade,
      v.shotValue,
      v.expectedPoints))

    val shots = reduceShots(shotsForReduction, dt, season)

    writeShots(NBATables.offense_expected_points_zoned, shots)
  }

  private def defenseTotal(scoredShot: Seq[ScoredShot], dt: String, season: String): Unit = {
    val shotsForReduction = scoredShot.map(v => ExpectedPointsForReduction(
      v.defenseTeamId,
      "Total",
      v.shotAttempted,
      v.shotMade,
      v.shotValue,
      v.expectedPoints))

    val shots = reduceShots(shotsForReduction, dt, season)

    writeShots(NBATables.defense_expected_points_total, shots)
  }

  private def defenseZoned(scoredShot: Seq[ScoredShot], dt: String, season: String): Unit = {
    val shotsForReduction = scoredShot.map(v => ExpectedPointsForReduction(
      v.defenseTeamId,
      v.bin,
      v.shotAttempted,
      v.shotMade,
      v.shotValue,
      v.expectedPoints))

    val shots = reduceShots(shotsForReduction, dt, season)

    writeShots(NBATables.defense_expected_points_zoned, shots)
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