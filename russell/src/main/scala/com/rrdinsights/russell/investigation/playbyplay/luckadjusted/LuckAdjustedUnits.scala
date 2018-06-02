package com.rrdinsights.russell.investigation.playbyplay.luckadjusted

import java.{lang => jl}

import com.rrdinsights.russell.investigation.shots.ShotUtils
import com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedPointsArguments
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel._
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.{MapJoin, TimeUtils}

object LuckAdjustedUnits {
  /**
    * This object is for calculating stints for luck adjusted RAPM
    *
    */
  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow
    val args = ExpectedPointsArguments(strings)
    val season = args.season

    val whereSeason = s"season = '$season'"

    val playByPlay = LuckAdjustedUtils.readPlayByPlay(whereSeason)

    println(s"${playByPlay.size} PlayByPlay Events <<<<<<<<<<<<<<<<<<")

    val freeThrowMap = LuckAdjustedUtils.buildPlayerCareerFreeThrowPercentMap()

    val seasonShots = ShotUtils.readScoredShots(whereSeason)

    val keyedPbP = playByPlay.map(v => ((v.gameId, v.eventNumber), v)).toMap
    val keyedShots = seasonShots.map(v => ((v.gameId, v.eventNumber), v)).toMap


    val playByPlayWithShotData = MapJoin.leftOuterJoin(keyedPbP, keyedShots)


    val units = playByPlayWithShotData
      .groupBy(v => (v._1.gameId, v._1.period))
      .flatMap(v => {
        LuckAdjustedUtils.seperatePossessions(v._2.sortBy(_._1))
          .flatMap(c => {
            println(c)
            val out = parsePossesions(v._1, c, freeThrowMap, dt)
            println(out)
            out
          })
      })
      .groupBy(_.primaryKey)
      .map(v => v._2.reduce(_ + _))
      .toSeq

    println(s"${units.length} <<<<<<<<< Units are stored")

    writeUnits(units)


  }

  println()

  private def writeUnits(stints: Seq[LuckAdjustedUnit]): Unit = {
    MySqlClient.createTable(NBATables.luck_adjusted_units)
    MySqlClient.insertInto(NBATables.luck_adjusted_units, stints)
  }


  private def parsePossesions(gameInfo: (String, Integer),
                              eventsWithTime: (Seq[(PlayByPlayWithLineup, Option[ScoredShot])], Integer),
                              freeThrowMap: Map[jl.Integer, jl.Double],
                              dt: String): Seq[LuckAdjustedUnit] = {

    val events = eventsWithTime._1
    val time = eventsWithTime._2
    val countedPoints = LuckAdjustedUtils.countPoints(events, freeThrowMap)

    val first = events.find(v => v._2.isDefined).getOrElse(events.head)
    val team1Points = countedPoints.find(_.teamId == first._1.teamId1)
    val team2Points = countedPoints.find(_.teamId == first._1.teamId2)

    val possession = LuckAdjustedUtils.determinePossession(events)
    val team1Possessions = if (possession == first._1.teamId1) 1 else 0
    val team2Possessions = if (possession == first._1.teamId2) 1 else 0

    val gameId = gameInfo._1
    val period = gameInfo._2
    val season = first._1.season

    val primaryKey1 = Seq(
      first._1.team1player1Id,
      first._1.team1player2Id,
      first._1.team1player3Id,
      first._1.team1player4Id,
      first._1.team1player5Id,
      gameId,
      period,
      season).mkString("_")

    val primaryKey2 = Seq(
      first._1.team2player1Id,
      first._1.team2player2Id,
      first._1.team2player3Id,
      first._1.team2player4Id,
      first._1.team2player5Id,
      gameId,
      period,
      season).mkString("_")

    Seq(
      LuckAdjustedUnit(
        primaryKey = primaryKey1,
        gameId = gameId,
        period = period,
        season = season,
        dt = dt,
        teamId = first._1.teamId1,
        player1Id = first._1.team1player1Id,
        player2Id = first._1.team1player2Id,
        player3Id = first._1.team1player3Id,
        player4Id = first._1.team1player4Id,
        player5Id = first._1.team1player5Id,
        points = team1Points.map(_.points).getOrElse(0.0),
        expectedPoints = team1Points.map(_.expectedPoints).getOrElse(0.0),
        fieldGoalAttempts = team1Points.map(_.fieldGoalAttempts).getOrElse(0),
        fieldGoals = team1Points.map(_.fieldGoals).getOrElse(0),
        threePtAttempts = team1Points.map(_.threePtAttempts).getOrElse(0),
        threePtMade = team1Points.map(_.threePtMade).getOrElse(0),
        freeThrowAttempts = team1Points.map(_.freeThrowAttempts).getOrElse(0),
        freeThrowsMade = team1Points.map(_.freeThrowsMade).getOrElse(0),
        possessions = team1Possessions,
        seconds = time
      ),
      LuckAdjustedUnit(
        primaryKey = primaryKey2,
        gameId = gameId,
        period = period,
        season = season,
        dt = dt,
        teamId = first._1.teamId2,
        player1Id = first._1.team2player1Id,
        player2Id = first._1.team2player2Id,
        player3Id = first._1.team2player3Id,
        player4Id = first._1.team2player4Id,
        player5Id = first._1.team2player5Id,
        points = team2Points.map(_.points).getOrElse(0.0),
        expectedPoints = team2Points.map(_.expectedPoints).getOrElse(0.0),
        fieldGoalAttempts = team2Points.map(_.fieldGoalAttempts).getOrElse(0),
        fieldGoals = team2Points.map(_.fieldGoals).getOrElse(0),
        threePtAttempts = team2Points.map(_.threePtAttempts).getOrElse(0),
        threePtMade = team2Points.map(_.threePtMade).getOrElse(0),
        freeThrowAttempts = team2Points.map(_.freeThrowAttempts).getOrElse(0),
        freeThrowsMade = team2Points.map(_.freeThrowsMade).getOrElse(0),
        possessions = team2Possessions,
        seconds = time))
  }

}

final case class LuckAdjustedUnit(primaryKey: String,
                                  gameId: String,
                                  period: Integer,
                                  season: String,
                                  dt: String,

                                  teamId: jl.Integer,
                                  player1Id: jl.Integer,
                                  player2Id: jl.Integer,
                                  player3Id: jl.Integer,
                                  player4Id: jl.Integer,
                                  player5Id: jl.Integer,

                                  points: jl.Double = 0.0,
                                  expectedPoints: jl.Double = 0.0,

                                  assists: jl.Integer = 0,

                                  defensiveRebounds: jl.Integer = 0,

                                  offensiveRebounds: jl.Integer = 0,

                                  fouls: jl.Integer = 0,

                                  turnovers: jl.Integer = 0,

                                  fieldGoalAttempts: jl.Integer = 0,
                                  fieldGoals: jl.Integer = 0,
                                  threePtAttempts: jl.Integer = 0,
                                  threePtMade: jl.Integer = 0,
                                  freeThrowAttempts: jl.Integer = 0,
                                  freeThrowsMade: jl.Integer = 0,

                                  possessions: jl.Integer = 0,
                                  seconds: jl.Integer = 0) {

  def +(other: LuckAdjustedUnit): LuckAdjustedUnit =
    LuckAdjustedUnit(
      primaryKey,
      gameId,
      period,
      season,
      dt,

      teamId,
      player1Id,
      player2Id,
      player3Id,
      player4Id,
      player5Id,

      points + other.points,
      expectedPoints + other.expectedPoints,

      assists + other.assists,

      defensiveRebounds + other.defensiveRebounds,

      offensiveRebounds + other.offensiveRebounds,

      fouls + other.fouls,

      turnovers + other.turnovers,

      fieldGoalAttempts + other.fieldGoalAttempts,
      fieldGoals + other.fieldGoals,
      threePtAttempts + other.threePtAttempts,
      threePtMade + other.threePtMade,
      freeThrowAttempts + other.freeThrowAttempts,
      freeThrowsMade + other.freeThrowsMade,

      possessions + other.possessions,
      seconds + other.seconds)
}