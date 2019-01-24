package com.rrdinsights.russell.investigation.playbyplay.luckadjusted

import java.sql.ResultSet
import java.{lang => jl}

import com.rrdinsights.russell.etl.application.GameSummaryMap
import com.rrdinsights.russell.investigation.GameDateMapper
import com.rrdinsights.russell.investigation.shots.ShotUtils
import com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedPointsArguments
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel._
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.{MapJoin, TimeUtils}

object LuckAdjustedPossessions {

  /**
    * This object is for calculating possessions for WOWY
    *
    */
  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow
    val args = ExpectedPointsArguments(strings)
    val season = args.season
    val seasonType = args.seasonType
    val whereSeason = s"season = '$season'"
    val whereSeasonType = s"seasontype = '$seasonType'"
    val playByPlay =
      LuckAdjustedUtils.readPlayByPlay(whereSeason, whereSeasonType)

    val freeThrowMap = LuckAdjustedUtils.buildPlayerCareerFreeThrowPercentMap()

    val seasonShots = ShotUtils.readScoredShots(whereSeason)

    val keyedPbP = playByPlay.map(v => ((v.gameId, v.eventNumber), v)).toMap
    val keyedShots = seasonShots.map(v => ((v.gameId, v.eventNumber), v)).toMap

    val playByPlayWithShotData = MapJoin.leftOuterJoin(keyedPbP, keyedShots)

    val possessions = playByPlayWithShotData
      .groupBy(v => (v._1.gameId, v._1.period))
      .flatMap(v => {
        val sorted = v._2.sortBy(_._1)
        LuckAdjustedUtils
          .seperatePossessions(sorted)
          .map(c => parsePossesions(c, sorted, freeThrowMap, dt))
      })
      .toSeq

    writePossessions(possessions)

    writeOneWayPossessions(possessions.flatMap(_.toOneWayPossessions))
  }

  private def writePossessions(stints: Seq[LuckAdjustedPossession]): Unit = {
    MySqlClient.createTable(NBATables.luck_adjusted_possessions)
    MySqlClient.insertInto(NBATables.luck_adjusted_possessions, stints)
  }

  private def writeOneWayPossessions(stints: Seq[OneWayPossession]): Unit = {
    MySqlClient.createTable(NBATables.luck_adjusted_one_way_possessions)
    MySqlClient.insertInto(NBATables.luck_adjusted_one_way_possessions, stints)
  }

  private def parsePossesions(eventsWithTime: (Seq[(PlayByPlayWithLineup,
                                                    Option[ScoredShot])],
                                               Integer),
                              allEvents: Seq[(PlayByPlayWithLineup,
                                              Option[ScoredShot])],
                              freeThrowMap: Map[jl.Integer, jl.Double],
                              dt: String): LuckAdjustedPossession = {

    val events = eventsWithTime._1
    val time = eventsWithTime._2
    val points = LuckAdjustedUtils.countPoints(events, freeThrowMap)
    val turnovers = LuckAdjustedUtils.countTurnovers(events)
    val rebounds = LuckAdjustedUtils.countRebounds(events, allEvents)

    val first = events.head
    val team1Points = points.find(_.teamId == first._1.teamId1)
    val team2Points = points.find(_.teamId == first._1.teamId2)
    val team1Turnovers =
      turnovers.find(_.teamId == first._1.teamId1).map(_.turnovers)
    val team2Turnovers =
      turnovers.find(_.teamId == first._1.teamId2).map(_.turnovers)
    val team1Rebounds = rebounds.find(_.teamId == first._1.teamId1)
    val team2Rebounds = rebounds.find(_.teamId == first._1.teamId2)

    val gameInfo = GameSummaryMap.GameMap(first._1.gameId)

    val team1Score =
      if (gameInfo.homeTeamId == first._1.teamId1) first._1.homeScore
      else first._1.awayScore
    val team2Score =
      if (gameInfo.homeTeamId == first._1.teamId1) first._1.awayScore
      else first._1.homeScore

    val primaryKey = Seq(
      first._1.eventNumber,
      first._1.gameId,
    ).mkString("_")

    val possession = LuckAdjustedUtils.determinePossession(events)
    val team1Possessions = if (possession == first._1.teamId1) 1 else 0
    val team2Possessions = if (possession == first._1.teamId2) 1 else 0

    LuckAdjustedPossession(
      primaryKey = primaryKey,
      season = first._1.season,
      seasonType = first._1.seasonType,
      gameId = first._1.gameId,
      period = first._1.period,
      gameDate = GameDateMapper.gameDate(first._1.gameId).get.gameDateInMillis,
      secondsElapsed = first._1.timeElapsed,
      team1Score = team1Score,
      team2Score = team2Score,
      dt = dt,
      teamId1 = first._1.teamId1,
      team1player1Id = first._1.team1player1Id,
      team1player2Id = first._1.team1player2Id,
      team1player3Id = first._1.team1player3Id,
      team1player4Id = first._1.team1player4Id,
      team1player5Id = first._1.team1player5Id,
      teamId2 = first._1.teamId2,
      team2player1Id = first._1.team2player1Id,
      team2player2Id = first._1.team2player2Id,
      team2player3Id = first._1.team2player3Id,
      team2player4Id = first._1.team2player4Id,
      team2player5Id = first._1.team2player5Id,
      team1Points = team1Points.map(_.points).getOrElse(0.0),
      team1ExpectedPoints = team1Points.map(_.expectedPoints).getOrElse(0.0),
      team2Points = team2Points.map(_.points).getOrElse(0.0),
      team2ExpectedPoints = team2Points.map(_.expectedPoints).getOrElse(0.0),
      team1DefensiveRebounds =
        team1Rebounds.map(_.defensiveRebounds).getOrElse(0),
      team2DefensiveRebounds =
        team2Rebounds.map(_.defensiveRebounds).getOrElse(0),
      team1OffensiveRebounds =
        team1Rebounds.map(_.offensiveRebounds).getOrElse(0),
      team2OffensiveRebounds =
        team2Rebounds.map(_.offensiveRebounds).getOrElse(0),
      team1Turnovers = team1Turnovers.getOrElse(0),
      team2Turnovers = team2Turnovers.getOrElse(0),
      team1FieldGoalAttempts = team1Points.map(_.fieldGoalAttempts).getOrElse(0),
      team1FieldGoals = team1Points.map(_.fieldGoals).getOrElse(0),
      team1ThreePtAttempts = team1Points.map(_.threePtAttempts).getOrElse(0),
      team1ThreePtMade = team1Points.map(_.threePtMade).getOrElse(0),
      team1FreeThrowAttempts = team1Points.map(_.freeThrowAttempts).getOrElse(0),
      team1FreeThrowsMade = team1Points.map(_.freeThrowsMade).getOrElse(0),
      team2FieldGoalAttempts = team2Points.map(_.fieldGoalAttempts).getOrElse(0),
      team2FieldGoals = team2Points.map(_.fieldGoals).getOrElse(0),
      team2ThreePtAttempts = team2Points.map(_.threePtAttempts).getOrElse(0),
      team2ThreePtMade = team2Points.map(_.threePtMade).getOrElse(0),
      team2FreeThrowAttempts = team2Points.map(_.freeThrowAttempts).getOrElse(0),
      team2FreeThrowsMade = team2Points.map(_.freeThrowsMade).getOrElse(0),
      team1Possessions = team1Possessions,
      team2Possessions = team2Possessions,
      seconds = time
    )
  }
}

final case class LuckAdjustedPossession(primaryKey: String,
                                        season: String,
                                        seasonType: String,
                                        gameId: String,
                                        gameDate: jl.Long,
                                        period: jl.Integer,
                                        secondsElapsed: jl.Integer,
                                        team1Score: jl.Integer,
                                        team2Score: jl.Integer,
                                        teamId1: jl.Integer,
                                        team1player1Id: jl.Integer,
                                        team1player2Id: jl.Integer,
                                        team1player3Id: jl.Integer,
                                        team1player4Id: jl.Integer,
                                        team1player5Id: jl.Integer,
                                        teamId2: jl.Integer,
                                        team2player1Id: jl.Integer,
                                        team2player2Id: jl.Integer,
                                        team2player3Id: jl.Integer,
                                        team2player4Id: jl.Integer,
                                        team2player5Id: jl.Integer,
                                        team1Points: jl.Double = 0.0,
                                        team1ExpectedPoints: jl.Double = 0.0,
                                        team2Points: jl.Double = 0.0,
                                        team2ExpectedPoints: jl.Double = 0.0,
                                        team1Assists: jl.Integer = 0,
                                        team2Assists: jl.Integer = 0,
                                        team1DefensiveRebounds: jl.Integer = 0,
                                        team2DefensiveRebounds: jl.Integer = 0,
                                        team1OffensiveRebounds: jl.Integer = 0,
                                        team2OffensiveRebounds: jl.Integer = 0,
                                        team1Fouls: jl.Integer = 0,
                                        team2Fouls: jl.Integer = 0,
                                        team1Turnovers: jl.Integer = 0,
                                        team2Turnovers: jl.Integer = 0,
                                        team1Possessions: jl.Integer = 0,
                                        team2Possessions: jl.Integer = 0,
                                        team1FieldGoalAttempts: jl.Integer = 0,
                                        team1FieldGoals: jl.Integer = 0,
                                        team1ThreePtAttempts: jl.Integer = 0,
                                        team1ThreePtMade: jl.Integer = 0,
                                        team1FreeThrowAttempts: jl.Integer = 0,
                                        team1FreeThrowsMade: jl.Integer = 0,
                                        team2FieldGoalAttempts: jl.Integer = 0,
                                        team2FieldGoals: jl.Integer = 0,
                                        team2ThreePtAttempts: jl.Integer = 0,
                                        team2ThreePtMade: jl.Integer = 0,
                                        team2FreeThrowAttempts: jl.Integer = 0,
                                        team2FreeThrowsMade: jl.Integer = 0,
                                        seconds: jl.Integer = 0,
                                        dt: String) {

  def toOneWayPossessions: Seq[OneWayPossession] =
    Seq(
      OneWayPossession(
        primaryKey = Seq(
          primaryKey,
          teamId1
        ).mkString("_"),
        season = season,
        seasonType = seasonType,
        gameId = gameId,
        gameDate = gameDate,
        period = period,
        secondsElapsed = secondsElapsed,
        team1Score = team1Score,
        team2Score = team2Score,
        dt = dt,
        offenseTeamId1 = teamId1,
        offensePlayer1Id = team1player1Id,
        offensePlayer2Id = team1player2Id,
        offensePlayer3Id = team1player3Id,
        offensePlayer4Id = team1player4Id,
        offensePlayer5Id = team1player5Id,
        defenseTeamId2 = teamId2,
        defensePlayer1Id = team2player1Id,
        defensePlayer2Id = team2player2Id,
        defensePlayer3Id = team2player3Id,
        defensePlayer4Id = team2player4Id,
        defensePlayer5Id = team2player5Id,
        points = team1Points,
        expectedPoints = team1ExpectedPoints,
        defensiveRebounds = team1DefensiveRebounds,
        offensiveRebounds = team1OffensiveRebounds,
        opponentDefensiveRebounds = team2DefensiveRebounds,
        opponentOffensiveRebounds = team2OffensiveRebounds,
        turnovers = team1Turnovers,
        fieldGoalAttempts = team1FieldGoalAttempts,
        fieldGoals = team1FieldGoals,
        threePtAttempts = team1ThreePtAttempts,
        threePtMade = team1ThreePtMade,
        freeThrowAttempts = team1FreeThrowAttempts,
        freeThrowsMade = team1FreeThrowsMade,
        possessions = team1Possessions,
        seconds = seconds
      ),
      OneWayPossession(
        primaryKey = Seq(
          primaryKey,
          teamId2
        ).mkString("_"),
        season = season,
        seasonType = seasonType,
        dt = dt,
        gameId = gameId,
        gameDate = gameDate,
        period = period,
        secondsElapsed = secondsElapsed,
        team1Score = team2Score,
        team2Score = team1Score,
        offenseTeamId1 = teamId2,
        offensePlayer1Id = team2player1Id,
        offensePlayer2Id = team2player2Id,
        offensePlayer3Id = team2player3Id,
        offensePlayer4Id = team2player4Id,
        offensePlayer5Id = team2player5Id,
        defenseTeamId2 = teamId1,
        defensePlayer1Id = team1player1Id,
        defensePlayer2Id = team1player2Id,
        defensePlayer3Id = team1player3Id,
        defensePlayer4Id = team1player4Id,
        defensePlayer5Id = team1player5Id,
        points = team2Points,
        expectedPoints = team2ExpectedPoints,
        defensiveRebounds = team2DefensiveRebounds,
        offensiveRebounds = team2OffensiveRebounds,
        opponentDefensiveRebounds = team1DefensiveRebounds,
        opponentOffensiveRebounds = team1OffensiveRebounds,
        turnovers = team2Turnovers,
        fieldGoalAttempts = team2FieldGoalAttempts,
        fieldGoals = team2FieldGoals,
        threePtAttempts = team2ThreePtAttempts,
        threePtMade = team2ThreePtMade,
        freeThrowAttempts = team2FreeThrowAttempts,
        freeThrowsMade = team2FreeThrowsMade,
        possessions = team2Possessions,
        seconds = seconds
      )
    ).filter(_.possessions > 0)
}

final case class OneWayPossession(primaryKey: String,
                                  season: String,
                                  seasonType: String,
                                  gameId: String,
                                  gameDate: jl.Long,
                                  period: jl.Integer,
                                  secondsElapsed: jl.Integer,
                                  team1Score: jl.Integer,
                                  team2Score: jl.Integer,
                                  offenseTeamId1: jl.Integer,
                                  offensePlayer1Id: jl.Integer,
                                  offensePlayer2Id: jl.Integer,
                                  offensePlayer3Id: jl.Integer,
                                  offensePlayer4Id: jl.Integer,
                                  offensePlayer5Id: jl.Integer,
                                  defenseTeamId2: jl.Integer,
                                  defensePlayer1Id: jl.Integer,
                                  defensePlayer2Id: jl.Integer,
                                  defensePlayer3Id: jl.Integer,
                                  defensePlayer4Id: jl.Integer,
                                  defensePlayer5Id: jl.Integer,
                                  points: jl.Double = 0.0,
                                  expectedPoints: jl.Double = 0.0,
                                  defensiveRebounds: jl.Integer = 0,
                                  offensiveRebounds: jl.Integer = 0,
                                  opponentDefensiveRebounds: jl.Integer = 0,
                                  opponentOffensiveRebounds: jl.Integer = 0,
                                  offensiveFouls: jl.Integer = 0,
                                  defensiveFouls: jl.Integer = 0,
                                  turnovers: jl.Integer = 0,
                                  fieldGoalAttempts: jl.Integer = 0,
                                  fieldGoals: jl.Integer = 0,
                                  threePtAttempts: jl.Integer = 0,
                                  threePtMade: jl.Integer = 0,
                                  freeThrowAttempts: jl.Integer = 0,
                                  freeThrowsMade: jl.Integer = 0,
                                  possessions: jl.Integer = 0,
                                  seconds: jl.Integer = 0,
                                  dt: String)

object OneWayPossession extends ResultSetMapper {

  def apply(resultSet: ResultSet): OneWayPossession =
    OneWayPossession(
      primaryKey = getString(resultSet, 0),
      season = getString(resultSet, 1),
      seasonType = getString(resultSet, 2),
      gameId = getString(resultSet, 3),
      gameDate = getLong(resultSet, 4),
      period = getInt(resultSet, 5),
      secondsElapsed = getInt(resultSet, 6),
      team1Score = getInt(resultSet, 7),
      team2Score = getInt(resultSet, 8),
      offenseTeamId1 = getInt(resultSet, 9),
      offensePlayer1Id = getInt(resultSet, 10),
      offensePlayer2Id = getInt(resultSet, 11),
      offensePlayer3Id = getInt(resultSet, 12),
      offensePlayer4Id = getInt(resultSet, 13),
      offensePlayer5Id = getInt(resultSet, 14),
      defenseTeamId2 = getInt(resultSet, 15),
      defensePlayer1Id = getInt(resultSet, 16),
      defensePlayer2Id = getInt(resultSet, 17),
      defensePlayer3Id = getInt(resultSet, 18),
      defensePlayer4Id = getInt(resultSet, 19),
      defensePlayer5Id = getInt(resultSet, 20),
      points = getDouble(resultSet, 21),
      expectedPoints = getDouble(resultSet, 22),
      defensiveRebounds = getInt(resultSet, 23),
      offensiveRebounds = getInt(resultSet, 24),
      opponentDefensiveRebounds = getInt(resultSet, 25),
      opponentOffensiveRebounds = getInt(resultSet, 26),
      offensiveFouls = getInt(resultSet, 27),
      defensiveFouls = getInt(resultSet, 28),
      turnovers = getInt(resultSet, 29),
      fieldGoalAttempts = getInt(resultSet, 30),
      fieldGoals = getInt(resultSet, 31),
      threePtAttempts = getInt(resultSet, 32),
      threePtMade = getInt(resultSet, 33),
      freeThrowAttempts = getInt(resultSet, 34),
      freeThrowsMade = getInt(resultSet, 35),
      possessions = getInt(resultSet, 36),
      seconds = getInt(resultSet, 37),
      dt = getString(resultSet, 38)
    )

}
