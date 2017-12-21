package com.rrdinsights.russell.investigation.shots.expectedshots

import java.{lang => jl}

import com.rrdinsights.russell.etl.application.GameLogDownloader
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{GameRecord, ScoredShot}
import com.rrdinsights.russell.storage.tables.{MySqlTable, NBATables}
import com.rrdinsights.russell.utils.TimeUtils

object ExpectedShotsByGameCalculator {

  import com.rrdinsights.russell.utils.MathUtils._

  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow
    val args = ExpectedPointsArguments(strings)
    val season = args.seasonOpt.getOrElse(throw new IllegalArgumentException("Must provide a season"))
    val where = Seq(s"season = '$season'")
    val scoredShots = readScoredShots(where)
    val games = GameLogDownloader.readGameLogs(season)
    val gameMap = buildGameMap(games)

    if (args.offense) {
      offenseTotal(scoredShots, gameMap, dt, season)
      if (args.zoned) {
        offenseZoned(scoredShots, gameMap, dt, season)
      }
    }

    if (args.defense) {
      defenseTotal(scoredShots, gameMap, dt, season)
      if (args.zoned) {
        defenseZoned(scoredShots, gameMap, dt, season)
      }
    }

  }

  private def buildGameMap(games: Seq[GameRecord]): Map[(Integer, String), (Int, Int)] =
    games
      .groupBy(_.teamId)
      .flatMap(v => sortGames(v._2))

  private def sortGames(games: Seq[GameRecord]): Seq[((Integer, String), (Int, Int))] = {
    games
      .map(v => (v.teamId, v.gameId, TimeUtils.parseGameLogDate(v.gameDate)))
      .sortBy(_._3)
      .zipWithIndex
      .map(v => ((v._1._1, v._1._2), (v._2+1, games.size)))
  }



  private def reduceShotGroup(key: (Integer, Integer, String), shots: Seq[ExpectedPointsForReductionByGame], dt: String, season: String): ExpectedPointsByGame = {
    val attempted = shots.map(v => v.shotAttempts.intValue()).sum
    val made = shots.map(v => v.shotMade.intValue()).sum
    val expectedPoints = shots.map(v => v.expectedPoints.doubleValue())
    val expectedPointsAvg = mean(expectedPoints)
    val expectedPointsStDev = stdDev(expectedPoints)
    val value = shots.head.shotValue
    val points = shots.map(v => v.shotMade * v.shotValue)
    val pointsAvg = mean(points)
    val pointsStDev = stdDev(points)

    ExpectedPointsByGame(
      s"${key._1}_${key._2}_${key._3}_$season",
      key._1,
      key._2,
      key._3,
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

  private def reduceShots(shots: Seq[ExpectedPointsForReductionByGame], dt: String, season: String): Seq[ExpectedPointsByGame] =
    shots
      .groupBy(v => (v.teamId, v.game, v.bin))
      .map(v => reduceShotGroup(v._1, v._2, dt, season))
      .toSeq


  private def offenseTotal(scoredShot: Seq[ScoredShot], gameMap: Map[(Integer, String), (Int, Int)], dt: String, season: String): Unit = {
    val shotsForReduction = scoredShot.flatMap(v => {
      val n = gameMap((v.offenseTeamId, v.gameId))
      buildShotsForReduction(v, v.offenseTeamId, n)
    })

    val shots = reduceShots(shotsForReduction, dt, season)

    writeShots(NBATables.offense_expected_points_by_game_total, shots)
  }

  def buildShotsForReduction(scoredShot: ScoredShot, teamId: Integer, n: (Int, Int)): Seq[ExpectedPointsForReductionByGame] =
    List.range(n._1, n._2+1)
      .map(v => ExpectedPointsForReductionByGame(
        teamId,
        v,
        "Total",
        scoredShot.shotAttempted,
        scoredShot.shotMade,
        scoredShot.shotValue,
        scoredShot.expectedPoints))

  def buildBinnedShotsForReduction(scoredShot: ScoredShot, teamId: Integer, n: (Int, Int)): Seq[ExpectedPointsForReductionByGame] =
    List.range(n._1, n._2+1)
      .map(v => ExpectedPointsForReductionByGame(
        teamId,
        v,
        scoredShot.bin,
        scoredShot.shotAttempted,
        scoredShot.shotMade,
        scoredShot.shotValue,
        scoredShot.expectedPoints))

  private def offenseZoned(scoredShot: Seq[ScoredShot], gameMap: Map[(Integer, String), (Int, Int)], dt: String, season: String): Unit = {
    val shotsForReduction = scoredShot.flatMap(v => {
      val n = gameMap((v.offenseTeamId, v.gameId))
      buildBinnedShotsForReduction(v, v.offenseTeamId, n)
    })

    val shots = reduceShots(shotsForReduction, dt, season)

    writeShots(NBATables.offense_expected_points_by_game_zoned, shots)
  }

  private def defenseTotal(scoredShot: Seq[ScoredShot], gameMap: Map[(Integer, String), (Int, Int)], dt: String, season: String): Unit = {
    val shotsForReduction = scoredShot.flatMap(v => {
      val n = gameMap((v.defenseTeamId, v.gameId))
      buildShotsForReduction(v, v.defenseTeamId, n)
    })

    val shots = reduceShots(shotsForReduction, dt, season)

    writeShots(NBATables.defense_expected_points_by_game_total, shots)
  }

  private def defenseZoned(scoredShot: Seq[ScoredShot], gameMap: Map[(Integer, String), (Int, Int)], dt: String, season: String): Unit = {
    val shotsForReduction = scoredShot.flatMap(v => {
      val n = gameMap((v.defenseTeamId, v.gameId))
      buildBinnedShotsForReduction(v, v.defenseTeamId, n)
    })

    val shots = reduceShots(shotsForReduction, dt, season)

    writeShots(NBATables.defense_expected_points_by_game_zoned, shots)
  }

  def writeShots(table: MySqlTable, shots: Seq[ExpectedPointsByGame]): Unit = {
    MySqlClient.createTable(table)
    MySqlClient.insertInto(table, shots)
  }

  private def readScoredShots(where: Seq[String]): Seq[ScoredShot] =
    MySqlClient.selectFrom(NBATables.team_scored_shots, ScoredShot.apply, where: _*)
}

final case class ExpectedPointsForReductionByGame(
                                                   teamId: Integer,
                                                   game: Integer,
                                                   bin: String,
                                                   shotAttempts: Integer,
                                                   shotMade: Integer,
                                                   shotValue: Integer,
                                                   expectedPoints: jl.Double)

final case class ExpectedPointsByGame(
                                       primaryKey: String,
                                       teamId: Integer,
                                       games: Integer,
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