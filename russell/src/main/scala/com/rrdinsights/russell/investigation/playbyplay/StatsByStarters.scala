package com.rrdinsights.russell.investigation.playbyplay

import java.{lang => jl}

import com.rrdinsights.russell.etl.application.PlayersOnCourtDownloader
import com.rrdinsights.russell.investigation.playbyplay.luckadjusted.OneWayPossession
import com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedPointsArguments
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.PlayersOnCourt
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.{MapJoin, TimeUtils}

object StatsByStarters {

  /**
    * This object is for calculating stats by starters
    *
    */
  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow
    val args = ExpectedPointsArguments(strings)
    val season = args.season
    val seasonType = args.seasonType
    val whereSeason = s"season = '$season'"
    val whereSeasonType = s"seasontype = '$seasonType'"

    val possessions =
      readPossessions(whereSeason, whereSeasonType).map(v => (v.gameId, v))
    val playersOnCourt = PlayersOnCourtDownloader
      .readPlayersOnCourtAtPeriod(whereSeason, whereSeasonType)
      .filter(_.period == 1)
      .map(v => (v.gameId, v))
      .toMap

    val possessionWithStarters = MapJoin
      .joinSeq(possessions, playersOnCourt)
      .map(mergePossessionWithStarters)

    writePossessionsWithStarters(possessionWithStarters)
  }

  def mergePossessionWithStarters(
      data: (OneWayPossession, PlayersOnCourt)): PossessionsWithStarters = {
    val oTeamId = data._1.offenseTeamId1
    val dTeamId = data._1.defenseTeamId2
    val oPlayers = Seq(data._1.offensePlayer1Id,
                       data._1.offensePlayer2Id,
                       data._1.offensePlayer3Id,
                       data._1.offensePlayer4Id,
                       data._1.offensePlayer5Id)
    val dPlayers = Seq(data._1.defensePlayer1Id,
                       data._1.defensePlayer2Id,
                       data._1.defensePlayer3Id,
                       data._1.defensePlayer4Id,
                       data._1.defensePlayer5Id)
    val oStarters = getTeamStarters(oTeamId, data._2)
    val dStarters = getTeamStarters(dTeamId, data._2)

    val numOStarters = oPlayers.intersect(oStarters).length
    val numDStarters = dPlayers.intersect(dStarters).length

    PossessionsWithStarters(
      primaryKey = data._1.primaryKey,
      season = data._1.season,
      seasonType = data._1.seasonType,
      gameId = data._1.gameId,
      gameDate = data._1.gameDate,
      period = data._1.period,
      secondsElapsed = data._1.secondsElapsed,
      team1Score = data._1.team1Score,
      team2Score = data._1.team2Score,
      offenseTeamId = data._1.offenseTeamId1,
      offenseTeamStarters = numOStarters,
      offensePlayer1Id = data._1.offensePlayer1Id,
      offensePlayer2Id = data._1.offensePlayer2Id,
      offensePlayer3Id = data._1.offensePlayer3Id,
      offensePlayer4Id = data._1.offensePlayer4Id,
      offensePlayer5Id = data._1.offensePlayer5Id,
      defenseTeamId = data._1.defenseTeamId2,
      defenseTeamStarters = numDStarters,
      defensePlayer1Id = data._1.defensePlayer1Id,
      defensePlayer2Id = data._1.defensePlayer2Id,
      defensePlayer3Id = data._1.defensePlayer3Id,
      defensePlayer4Id = data._1.defensePlayer4Id,
      defensePlayer5Id = data._1.defensePlayer5Id,
      points = data._1.points,
      expectedPoints = data._1.expectedPoints,
      defensiveRebounds = data._1.defensiveRebounds,
      offensiveRebounds = data._1.offensiveRebounds,
      opponentDefensiveRebounds = data._1.opponentDefensiveRebounds,
      opponentOffensiveRebounds = data._1.opponentOffensiveRebounds,
      offensiveFouls = data._1.offensiveFouls,
      defensiveFouls = data._1.defensiveFouls,
      turnovers = data._1.turnovers,
      fieldGoalAttempts = data._1.fieldGoalAttempts,
      fieldGoals = data._1.fieldGoals,
      threePtAttempts = data._1.threePtAttempts,
      threePtMade = data._1.threePtMade,
      freeThrowAttempts = data._1.freeThrowAttempts,
      freeThrowsMade = data._1.freeThrowsMade,
      possessions = data._1.possessions,
      seconds = data._1.seconds
    )

  }

  private def getTeamStarters(
      teamId: jl.Integer,
      playersOnCourt: PlayersOnCourt): Seq[jl.Integer] = {
    if (teamId == playersOnCourt.teamId1) {
      Seq(playersOnCourt.team1player1Id,
          playersOnCourt.team1player2Id,
          playersOnCourt.team1player3Id,
          playersOnCourt.team1player4Id,
          playersOnCourt.team1player5Id)
    } else {
      Seq(playersOnCourt.team2player1Id,
          playersOnCourt.team2player2Id,
          playersOnCourt.team2player3Id,
          playersOnCourt.team2player4Id,
          playersOnCourt.team2player5Id)

    }
  }

  def readPossessions(where: String*): Seq[OneWayPossession] =
    MySqlClient.selectFrom(NBATables.luck_adjusted_one_way_possessions,
                           OneWayPossession.apply,
                           where: _*)

  private def writePossessionsWithStarters(
      stints: Seq[PossessionsWithStarters]): Unit = {
    MySqlClient.createTable(NBATables.luck_adjusted_one_way_possessions_with_starters)
    MySqlClient.insertInto(NBATables.luck_adjusted_one_way_possessions_with_starters, stints)
  }
}

final case class PossessionsWithStarters(
    primaryKey: String,
    season: String,
    seasonType: String,
    gameId: String,
    gameDate: jl.Long,
    period: jl.Integer,
    secondsElapsed: jl.Integer,
    team1Score: jl.Integer,
    team2Score: jl.Integer,
    offenseTeamId: jl.Integer,
    offenseTeamStarters: jl.Integer,
    offensePlayer1Id: jl.Integer,
    offensePlayer2Id: jl.Integer,
    offensePlayer3Id: jl.Integer,
    offensePlayer4Id: jl.Integer,
    offensePlayer5Id: jl.Integer,
    defenseTeamId: jl.Integer,
    defenseTeamStarters: jl.Integer,
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
    seconds: jl.Integer = 0)
