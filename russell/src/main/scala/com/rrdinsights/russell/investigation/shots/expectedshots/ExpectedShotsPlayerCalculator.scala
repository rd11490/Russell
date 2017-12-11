package com.rrdinsights.russell.investigation.shots.expectedshots

import java.{lang => jl}

import com.rrdinsights.russell.commandline.{CommandLineBase, SeasonOption}
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{ExpectedPoints, ScoredShot}
import com.rrdinsights.russell.storage.tables.{MySqlTable, NBATables}
import com.rrdinsights.russell.utils.TimeUtils
import org.apache.commons.cli

object ExpectedShotsPlayerCalculator {

  import com.rrdinsights.russell.utils.MathUtils._

  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow
    val args = ExpectedPointsArguments(strings)
    val season = args.season.getOrElse(throw new IllegalArgumentException("Must provide a season"))
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

  private def reduceShotGroup(key: (Integer, String), shots: Seq[ExpectedPointsForReductionPlayer], dt: String, season: String): ExpectedPointsPlayer = {
    val attempted = shots.map(v => v.shotAttempts.intValue()).sum
    val made = shots.map(v => v.shotMade.intValue()).sum
    val expectedPoints = shots.map(v => v.expectedPoints.doubleValue())
    val expectedPointsAvg = mean(expectedPoints)
    val expectedPointsStDev = stdDev(expectedPoints)
    val value = shots.head.shotValue
    val points = shots.map(v => v.shotMade * v.shotValue)
    val pointsAvg = mean(points)
    val pointsStDev = stdDev(points)

    ExpectedPointsPlayer(
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

  private def reduceShots(shots: Seq[ExpectedPointsForReductionPlayer], dt: String, season: String): Seq[ExpectedPointsPlayer] =
    shots
      .groupBy(v => (v.id, v.bin))
      .map(v => reduceShotGroup(v._1, v._2, dt, season))
      .toSeq


  private def offenseTotal(scoredShot: Seq[ScoredShot], dt: String, season: String): Unit = {
    val shotsForReduction = scoredShot.flatMap(explodeScoredShotOffense)
    val shots = reduceShots(shotsForReduction, dt, season)

    writeShots(NBATables.offense_expected_points_by_player_total, shots)
  }

  private def explodeScoredShotOffense(scoredShot: ScoredShot): Seq[ExpectedPointsForReductionPlayer] = {
    Seq(
      scoredShot.offensePlayer1Id,
      scoredShot.offensePlayer2Id,
      scoredShot.offensePlayer3Id,
      scoredShot.offensePlayer4Id,
      scoredShot.offensePlayer5Id)
      .map(v =>
        ExpectedPointsForReductionPlayer(
          v,
          "Total",
          scoredShot.shotAttempted,
          scoredShot.shotMade,
          scoredShot.shotValue,
          scoredShot.expectedPoints))
  }

  private def offenseZoned(scoredShot: Seq[ScoredShot], dt: String, season: String): Unit = {
    val shotsForReduction = scoredShot.flatMap(explodeScoredShotOffense)

    val shots = reduceShots(shotsForReduction, dt, season)

    writeShots(NBATables.offense_expected_points_by_player_zoned, shots)
  }

  private def defenseTotal(scoredShot: Seq[ScoredShot], dt: String, season: String): Unit = {
    val shotsForReduction = scoredShot.flatMap(explodeScoredShotDefense)

    val shots = reduceShots(shotsForReduction, dt, season)

    writeShots(NBATables.defense_expected_points_by_player_total, shots)
  }

  private def defenseZoned(scoredShot: Seq[ScoredShot], dt: String, season: String): Unit = {
    val shotsForReduction = scoredShot.flatMap(explodeScoredShotDefense)

    val shots = reduceShots(shotsForReduction, dt, season)

    writeShots(NBATables.defense_expected_points_by_player_zoned, shots)
  }

  private def explodeScoredShotDefense(scoredShot: ScoredShot): Seq[ExpectedPointsForReductionPlayer] = {
    Seq(
      scoredShot.defensePlayer1Id,
      scoredShot.defensePlayer2Id,
      scoredShot.defensePlayer3Id,
      scoredShot.defensePlayer4Id,
      scoredShot.defensePlayer5Id)
      .map(v =>
        ExpectedPointsForReductionPlayer(
          v,
          "Total",
          scoredShot.shotAttempted,
          scoredShot.shotMade,
          scoredShot.shotValue,
          scoredShot.expectedPoints))
  }

  def writeShots(table: MySqlTable, shots: Seq[ExpectedPointsPlayer]): Unit = {
    MySqlClient.createTable(table)
    MySqlClient.insertInto(table, shots)
  }

  private def readScoredShots(where: Seq[String]): Seq[ScoredShot] =
    MySqlClient.selectFrom(NBATables.team_scored_shots, ScoredShot.apply, where: _*)
}

final case class ExpectedPointsForReductionPlayer(
                                                   id: Integer,
                                                   bin: String,
                                                   shotAttempts: Integer,
                                                   shotMade: Integer,
                                                   shotValue: Integer,
                                                   expectedPoints: jl.Double)

final case class ExpectedPointsPlayer(
                                       primaryKey: String,
                                       id: Integer,
                                       bin: String,
                                       attempts: Integer,
                                       made: Integer,
                                       value: Integer,
                                       pointsAvg: jl.Double,
                                       pointsStDev: jl.Double,
                                       expectedPointsAvg: jl.Double,
                                       expectedPointsStDev: jl.Double,
                                       season: String,
                                       dt: String)