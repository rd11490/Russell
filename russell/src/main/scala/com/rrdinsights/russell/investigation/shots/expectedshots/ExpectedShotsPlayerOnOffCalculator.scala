package com.rrdinsights.russell.investigation.shots.expectedshots

import java.{lang => jl}

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{RosterPlayer, ScoredShot}
import com.rrdinsights.russell.storage.tables.{MySqlTable, NBATables}
import com.rrdinsights.russell.utils.TimeUtils

object ExpectedShotsPlayerOnOffCalculator {

  import com.rrdinsights.russell.utils.MathUtils._

  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow
    val args = ExpectedPointsArguments(strings)
    val season = args.seasonOpt.getOrElse(throw new IllegalArgumentException("Must provide a season"))
    val where = Seq(s"season = '$season'")
    val scoredShots = readScoredShots(where)
    val playerMap = buildRosterMap(readRosters(where))

    println(scoredShots.size)
    println(playerMap.size)

    if (args.offense) {
      calculateOnOffForOffense(scoredShots, playerMap, dt, season)
    }

    if (args.defense) {
      calculateOnOffForDefense(scoredShots, playerMap, dt, season)
    }

  }

  private def buildRosterMap(players: Seq[RosterPlayer]): Map[Integer, Seq[Integer]] =
    players.groupBy(_.teamId).map(v => (v._1, v._2.map(_.playerId)))

  private def reduceShotGroup(key: (Integer, String, String), shots: Seq[ExpectedPointsForReductionPlayerOnOff], dt: String, season: String): ExpectedPointsPlayerOnOff = {
    val attempted = shots.map(v => v.shotAttempts.intValue()).sum
    val made = shots.map(v => v.shotMade.intValue()).sum
    val expectedPoints = shots.map(v => v.expectedPoints.doubleValue())
    val expectedPointsAvg = mean(expectedPoints)
    val expectedPointsStDev = stdDev(expectedPoints)
    val value = shots.head.shotValue
    val points = shots.map(v => v.shotMade * v.shotValue)
    val pointsAvg = mean(points)
    val pointsStDev = stdDev(points)

    ExpectedPointsPlayerOnOff(
      s"${key._1}_${key._2}_${key._3}$season",
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

  private def reduceShots(shots: Seq[ExpectedPointsForReductionPlayerOnOff], dt: String, season: String): Seq[ExpectedPointsPlayerOnOff] =
    shots
      .groupBy(v => (v.id, v.bin, v.onOff))
      .map(v => reduceShotGroup(v._1, v._2, dt, season))
      .toSeq

  private def explodeScoredShotOffense(scoredShot: ScoredShot, rosters: Map[Integer, Seq[Integer]]): Seq[ExpectedPointsForReductionPlayerOnOff] = {
    val onCourt = Seq(
      scoredShot.offensePlayer1Id,
      scoredShot.offensePlayer2Id,
      scoredShot.offensePlayer3Id,
      scoredShot.offensePlayer4Id,
      scoredShot.offensePlayer5Id)

    val offCourt = rosters(scoredShot.offenseTeamId).diff(onCourt)
    onCourt.flatMap(v => buildExpectedPointsForReductionPlayerOnOff(scoredShot, v, "On")) ++
      offCourt.flatMap(v => buildExpectedPointsForReductionPlayerOnOff(scoredShot, v, "Off"))
  }

  private def buildExpectedPointsForReductionPlayerOnOff(scoredShot: ScoredShot, id: Integer, onOff: String): Seq[ExpectedPointsForReductionPlayerOnOff] =
    Seq(
      ExpectedPointsForReductionPlayerOnOff(
        id,
        scoredShot.bin,
        onOff = onOff,
        scoredShot.shotAttempted,
        scoredShot.shotMade,
        scoredShot.shotValue,
        scoredShot.expectedPoints),
      ExpectedPointsForReductionPlayerOnOff(
        id,
        "Total",
        onOff = onOff,
        scoredShot.shotAttempted,
        scoredShot.shotMade,
        scoredShot.shotValue,
        scoredShot.expectedPoints))

  private def calculateOnOffForOffense(scoredShot: Seq[ScoredShot], rosters: Map[Integer, Seq[Integer]], dt: String, season: String): Unit = {
    val shotsForReduction = scoredShot.flatMap(v => explodeScoredShotOffense(v, rosters))

    val shots = reduceShots(shotsForReduction, dt, season)

    writeShots(NBATables.offense_expected_points_by_player_on_off, shots)
  }

  private def calculateOnOffForDefense(scoredShot: Seq[ScoredShot], rosters: Map[Integer, Seq[Integer]], dt: String, season: String): Unit = {
    val shotsForReduction = scoredShot.flatMap(v => explodeScoredShotDefense(v, rosters))

    val shots = reduceShots(shotsForReduction, dt, season)

    writeShots(NBATables.defense_expected_points_by_player_on_off, shots)
  }

  private def explodeScoredShotDefense(scoredShot: ScoredShot, rosters: Map[Integer, Seq[Integer]], total: Boolean = false): Seq[ExpectedPointsForReductionPlayerOnOff] = {
    val onCourt = Seq(
      scoredShot.defensePlayer1Id,
      scoredShot.defensePlayer2Id,
      scoredShot.defensePlayer3Id,
      scoredShot.defensePlayer4Id,
      scoredShot.defensePlayer5Id)

    val offCourt = rosters(scoredShot.defenseTeamId).diff(onCourt)

    onCourt.map(v =>
      ExpectedPointsForReductionPlayerOnOff(
        v,
        if (total) "Total" else scoredShot.bin,
        onOff = "On",
        scoredShot.shotAttempted,
        scoredShot.shotMade,
        scoredShot.shotValue,
        scoredShot.expectedPoints)) ++
      offCourt.map(v =>
        ExpectedPointsForReductionPlayerOnOff(
          v,
          if (total) "Total" else scoredShot.bin,
          onOff = "Off",
          scoredShot.shotAttempted,
          scoredShot.shotMade,
          scoredShot.shotValue,
          scoredShot.expectedPoints))
  }

  def writeShots(table: MySqlTable, shots: Seq[ExpectedPointsPlayerOnOff]): Unit = {
    MySqlClient.createTable(table)
    MySqlClient.insertInto(table, shots)
  }

  private def readScoredShots(where: Seq[String]): Seq[ScoredShot] =
    MySqlClient.selectFrom(NBATables.team_scored_shots, ScoredShot.apply, where: _*)

  private def readRosters(where: Seq[String]): Seq[RosterPlayer] =
    MySqlClient.selectFrom(NBATables.roster_player, RosterPlayer.apply, where: _*)
}

final case class ExpectedPointsForReductionPlayerOnOff(
                                                        id: Integer,
                                                        bin: String,
                                                        onOff: String,
                                                        shotAttempts: Integer,
                                                        shotMade: Integer,
                                                        shotValue: Integer,
                                                        expectedPoints: jl.Double)

final case class ExpectedPointsPlayerOnOff(
                                            primaryKey: String,
                                            id: Integer,
                                            bin: String,
                                            onOff: String,
                                            attempts: Integer,
                                            made: Integer,
                                            value: Integer,
                                            pointsAvg: jl.Double,
                                            pointsStDev: jl.Double,
                                            expectedPointsAvg: jl.Double,
                                            expectedPointsStDev: jl.Double,
                                            season: String,
                                            dt: String)