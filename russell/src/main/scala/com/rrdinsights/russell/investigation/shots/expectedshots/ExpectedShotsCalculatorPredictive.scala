package com.rrdinsights.russell.investigation.shots.expectedshots

import com.rrdinsights.russell.etl.application.GameLogDownloader
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.ScoredShot
import com.rrdinsights.russell.storage.tables.{MySqlTable, NBATables}
import com.rrdinsights.russell.utils.TimeUtils

object ExpectedShotsCalculatorPredictive {
  /*

  import com.rrdinsights.russell.utils.MathUtils._

  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow
    val args = ExpectedPointsArguments(strings)
    val season = args.season.getOrElse(throw new IllegalArgumentException("Must provide a season"))
    val where = Seq(s"season = '$season'")
    val scoredShotsUnfiltered = readScoredShots(where)

    val where18 = Seq(s"season = '2017-18'")

    val games18 = readScoredShots(where18)
      .map(v => v.gameId)
      .distinct
      .size

    val gameIds = GameLogDownloader.readGameLogs(season)
      .map(v => (v.gameId, TimeUtils.parseGameLogDate(v.gameDate)))
      .distinct
      .sortBy(v => v._2)
      .take(games18)
      .map(_._1)

    val scoredShots = scoredShotsUnfiltered.filter(v => gameIds.contains(v.gameId))

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

    writeShots(NBATables.offense_expected_points_total_predict, shots)
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

    writeShots(NBATables.offense_expected_points_zoned_predict, shots)
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

    writeShots(NBATables.defense_expected_points_total_predict, shots)
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

    writeShots(NBATables.defense_expected_points_zoned_predict, shots)
  }

  def writeShots(table: MySqlTable, shots: Seq[ExpectedPoints]): Unit = {
    MySqlClient.createTable(table)
    MySqlClient.insertInto(table, shots)
  }

  private def readScoredShots(where: Seq[String]): Seq[ScoredShot] =
    MySqlClient.selectFrom(NBATables.team_scored_shots, ScoredShot.apply, where: _*)
    */
}