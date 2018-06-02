package com.rrdinsights.russell.investigation.playbyplay

import java.{lang => jl}

import com.rrdinsights.russell.investigation.PlayerMapper
import com.rrdinsights.russell.investigation.playbyplay.luckadjusted.LuckAdjustedUtils
import com.rrdinsights.russell.investigation.shots.ShotUtils
import com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedPointsArguments
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{PlayByPlayEventMessageType, PlayByPlayWithLineup, ScoredShot}
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.{MapJoin, TimeUtils}

object PlayStatsByUnit {

  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow
    val args = ExpectedPointsArguments(strings)
    val season = args.season


    val whereSeason = s"season = '$season'"

    val playByPlay = LuckAdjustedUtils.readPlayByPlay(whereSeason)
    val seasonShots = ShotUtils.readScoredShots(whereSeason)

    println(s"${playByPlay.size} PlayByPlay Events <<<<<<<<<<<<<<<<<<")

    val keyedPbP = playByPlay.map(v => ((v.gameId, v.eventNumber), v)).toMap
    val keyedShots = seasonShots.map(v => ((v.gameId, v.eventNumber), v)).toMap

    val playByPlayWithShotData = MapJoin.leftOuterJoin(keyedPbP, keyedShots)

    val units = playByPlayWithShotData.groupBy(v => (v._1.gameId, v._1.period))
      .flatMap(v => LuckAdjustedUtils.seperatePossessions(v._2.sortBy(_._1)))
      .flatMap(v => parsePossession(v))
      .groupBy(_.primaryKey)
      .map(v => v._2.reduce(_ + _))
      .map(v => v.copy(dt = dt))
      .toSeq

    writePlayStatsByUnit(units)

  }

  def writePlayStatsByUnit(units: Seq[UnitPlayerStats]): Unit = {
    MySqlClient.createTable(NBATables.player_stats_by_unit)
    MySqlClient.insertInto(NBATables.player_stats_by_unit, units)
  }

  def parsePossession(possession: (Seq[(PlayByPlayWithLineup, Option[ScoredShot])], Integer)): Seq[UnitPlayerStats] = {
    possession
      ._1
      .flatMap(v => parseEvent(v))
      .groupBy(_.primaryKey)
      .map(v => v._2.reduce(_ + _))
      .map(v => v.copy(seconds = possession._2))
      .map(_.determinePossession)
      .toSeq
  }

  def extractPlayers(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[Integer] =
    if (event._1.teamId1 == event._1.player1TeamId) {
      Seq(
        event._1.team1player1Id,
        event._1.team1player2Id,
        event._1.team1player3Id,
        event._1.team1player4Id,
        event._1.team1player5Id)
    } else {
      Seq(
        event._1.team2player1Id,
        event._1.team2player2Id,
        event._1.team2player3Id,
        event._1.team2player4Id,
        event._1.team2player5Id)
    }

  def extractOppPlayers(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[Integer] =
    if (event._1.teamId1 != event._1.player1TeamId) {
      Seq(
        event._1.team1player1Id,
        event._1.team1player2Id,
        event._1.team1player3Id,
        event._1.team1player4Id,
        event._1.team1player5Id)
    } else {
      Seq(
        event._1.team2player1Id,
        event._1.team2player2Id,
        event._1.team2player3Id,
        event._1.team2player4Id,
        event._1.team2player5Id)
    }


  def getPlayerNames(players: Seq[Integer]): Seq[String] =
    players.map(v => PlayerMapper.lookup(v))

  def parseEvent(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    PlayByPlayEventMessageType.valueOf(event._1.playType) match {
      case PlayByPlayEventMessageType.Make => parseMakeEvent(event)
      case PlayByPlayEventMessageType.Miss => parseMissEvent(event)
      case PlayByPlayEventMessageType.Foul => parseFoulEvent(event)
      case PlayByPlayEventMessageType.FreeThrow => parseFreeThrow(event)
      case PlayByPlayEventMessageType.Turnover => parseTurnoverEvent(event)
      case PlayByPlayEventMessageType.Rebound => parseRebound(event)
      case _ => Seq.empty
    }
  }

  def parseMakeEvent(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    if (PlayByPlayUtils.isAssisted(event)) {
      parseMake(event) ++ parseAssist(event)
    } else {
      parseMake(event)
    }

  }

  def parseMake(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    val assisted = PlayByPlayUtils.isAssisted(event)

    val players = extractPlayers(event)
    val playerNames = getPlayerNames(players)

    val playerId = event._1.player1Id
    val playerName = PlayerMapper.lookup(playerId)
    val teamId = event._1.player1TeamId

    val season = event._1.season

    val primaryKey = buildPrimaryKey(playerId, players, season)

    val is3PTShot = PlayByPlayUtils.is3PtShot(event)

    val points = if (is3PTShot) 3 else 2

    val assistedFieldGoals = if (assisted) 1 else 0

    val fieldGoalAttempts = 1
    val fieldGoals = 1

    val threePtAttempts = if (is3PTShot) 1 else 0
    val threePtMade = if (is3PTShot) 1 else 0
    val assisted3PtMade = if (is3PTShot && assisted) 1 else 0

    val makeStats = UnitPlayerStats(
      primaryKey = primaryKey,
      season = season,
      dt = "",

      playerId = playerId,
      playerName = playerName,

      teamId = teamId,
      player1Id = players.head,
      player2Id = players(1),
      player3Id = players(2),
      player4Id = players(3),
      player5Id = players(4),

      player1Name = playerNames.head,
      player2Name = playerNames(1),
      player3Name = playerNames(2),
      player4Name = playerNames(3),
      player5Name = playerNames(4),

      points = points,
      assistedFieldGoals = assistedFieldGoals,
      fieldGoalAttempts = fieldGoalAttempts,
      fieldGoals = fieldGoals,
      threePtAttempts = threePtAttempts,
      threePtMade = threePtMade,
      assisted3PtMade = assisted3PtMade)

    val oppoPlayers = extractOppPlayers(event)
    val oppoTeam =  if (event._1.teamId1 == teamId) event._1.teamId2 else event._1.teamId1


    val offensePlayers = participatedInPossession(players, teamId, season, true)
    val defensePlayers = participatedInPossession(oppoPlayers, oppoTeam, season, false)

    makeStats +: (offensePlayers ++ defensePlayers)
  }

  def parseAssist(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    val players = extractPlayers(event)
    val playerNames = getPlayerNames(players)

    val playerId = event._1.player2Id
    val playerName = PlayerMapper.lookup(playerId)
    val teamId = event._1.player2TeamId

    val season = event._1.season

    val primaryKey = buildPrimaryKey(playerId, players, season)

    val assistStats = UnitPlayerStats(
      primaryKey = primaryKey,
      season = season,
      dt = "",

      playerId = playerId,
      playerName = playerName,

      teamId = teamId,
      player1Id = players.head,
      player2Id = players(1),
      player3Id = players(2),
      player4Id = players(3),
      player5Id = players(4),

      player1Name = playerNames.head,
      player2Name = playerNames(1),
      player3Name = playerNames(2),
      player4Name = playerNames(3),
      player5Name = playerNames(4),

      assists = 1)

    val oppoPlayers = extractOppPlayers(event)
    val oppoTeam =  if (event._1.teamId1 == teamId) event._1.teamId2 else event._1.teamId1


    val offensePlayers = participatedInPossession(players, teamId, season, true)
    val defensePlayers = participatedInPossession(oppoPlayers, oppoTeam, season, false)

    assistStats +: (offensePlayers ++ defensePlayers)
  }


  def parseMissEvent(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    if (PlayByPlayUtils.isBlock(event)) {
      parseMiss(event) ++ parseBlock(event)
    } else {
      parseMiss(event)
    }
  }

  def parseMiss(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    val players = extractPlayers(event)
    val playerNames = getPlayerNames(players)

    val playerId = event._1.player1Id
    val playerName = PlayerMapper.lookup(playerId)
    val teamId = event._1.player1TeamId

    val season = event._1.season

    val primaryKey = buildPrimaryKey(playerId, players, season)

    val is3PTShot = PlayByPlayUtils.is3PtShot(event)

    val fieldGoalAttempts = 1
    val threePtAttempts = if (is3PTShot) 1 else 0

    val missStats = UnitPlayerStats(
      primaryKey = primaryKey,
      season = season,
      dt = "",

      playerId = playerId,
      playerName = playerName,

      teamId = teamId,
      player1Id = players.head,
      player2Id = players(1),
      player3Id = players(2),
      player4Id = players(3),
      player5Id = players(4),

      player1Name = playerNames.head,
      player2Name = playerNames(1),
      player3Name = playerNames(2),
      player4Name = playerNames(3),
      player5Name = playerNames(4),

      fieldGoalAttempts = fieldGoalAttempts,
      threePtAttempts = threePtAttempts)

    val oppoPlayers = extractOppPlayers(event)
    val oppoTeam =  if (event._1.teamId1 == teamId) event._1.teamId2 else event._1.teamId1

    val offensePlayers = participatedInPossession(players, teamId, season, true)
    val defensePlayers = participatedInPossession(oppoPlayers, oppoTeam, season, false)

    missStats +: (offensePlayers ++ defensePlayers)
  }

  def parseBlock(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    val players = extractOppPlayers(event)
    val playerNames = getPlayerNames(players)

    val playerId = event._1.player3Id
    val playerName = PlayerMapper.lookup(playerId)
    val teamId = event._1.player2TeamId

    val season = event._1.season

    val primaryKey = buildPrimaryKey(playerId, players, season)

    val blockStats = UnitPlayerStats(
      primaryKey = primaryKey,
      season = season,
      dt = "",

      playerId = playerId,
      playerName = playerName,

      teamId = teamId,
      player1Id = players.head,
      player2Id = players(1),
      player3Id = players(2),
      player4Id = players(3),
      player5Id = players(4),

      player1Name = playerNames.head,
      player2Name = playerNames(1),
      player3Name = playerNames(2),
      player4Name = playerNames(3),
      player5Name = playerNames(4),

      blocks = 1)

    val oppoPlayers = extractPlayers(event)
    val oppoTeam =  if (event._1.teamId1 == teamId) event._1.teamId2 else event._1.teamId1

    val offensePlayers = participatedInPossession(players, teamId, season, false)
    val defensePlayers = participatedInPossession(oppoPlayers, oppoTeam, season, true)

    blockStats +: (offensePlayers ++ defensePlayers)
  }

  def parseFoulEvent(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    parseFoulCommitted(event) ++ parseFoulDrawn(event)
  }

  def parseFoulCommitted(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    val players = extractPlayers(event)
    val playerNames = getPlayerNames(players)

    val playerId = event._1.player1Id
    val playerName = PlayerMapper.lookup(playerId)
    val teamId = event._1.player1TeamId

    val season = event._1.season

    val primaryKey = buildPrimaryKey(playerId, players, season)

    val foulCommitedStats = UnitPlayerStats(
      primaryKey = primaryKey,
      season = season,
      dt = "",

      playerId = playerId,
      playerName = playerName,

      teamId = teamId,
      player1Id = players.head,
      player2Id = players(1),
      player3Id = players(2),
      player4Id = players(3),
      player5Id = players(4),

      player1Name = playerNames.head,
      player2Name = playerNames(1),
      player3Name = playerNames(2),
      player4Name = playerNames(3),
      player5Name = playerNames(4),

      fouls = 1)

    val oppoPlayers = extractOppPlayers(event)
    val oppoTeam =  if (event._1.teamId1 == teamId) event._1.teamId2 else event._1.teamId1

    //Offensive Fouls: 26, 4
    val offesniveFoul = PlayByPlayUtils.isOffensiveFoul(event)

    val offensePlayers = participatedInPossession(players, teamId, season, offesniveFoul)
    val defensePlayers = participatedInPossession(oppoPlayers, oppoTeam, season, !offesniveFoul)

    foulCommitedStats +: (offensePlayers ++ defensePlayers)
  }

  def parseFoulDrawn(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    val players = extractOppPlayers(event)
    val playerNames = getPlayerNames(players)

    val playerId = event._1.player2Id
    val playerName = PlayerMapper.lookup(playerId)
    val teamId = event._1.player2TeamId

    val season = event._1.season

    val primaryKey = buildPrimaryKey(playerId, players, season)

    val foulCommitedStats = UnitPlayerStats(
      primaryKey = primaryKey,
      season = season,
      dt = "",

      playerId = playerId,
      playerName = playerName,

      teamId = teamId,
      player1Id = players.head,
      player2Id = players(1),
      player3Id = players(2),
      player4Id = players(3),
      player5Id = players(4),

      player1Name = playerNames.head,
      player2Name = playerNames(1),
      player3Name = playerNames(2),
      player4Name = playerNames(3),
      player5Name = playerNames(4),

      foulsDrawn = 1)

    val oppoPlayers = extractOppPlayers(event)
    val oppoTeam =  if (event._1.teamId1 == teamId) event._1.teamId2 else event._1.teamId1

    //Offensive Fouls: 26, 4
    val offesniveFoul = PlayByPlayUtils.isOffensiveFoul(event)

    val offensePlayers = participatedInPossession(players, teamId, season, !offesniveFoul)
    val defensePlayers = participatedInPossession(oppoPlayers, oppoTeam, season, offesniveFoul)

    foulCommitedStats +: (offensePlayers ++ defensePlayers)
  }

  def parseFreeThrow(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    val players = extractPlayers(event)
    val playerNames = getPlayerNames(players)

    val playerId = event._1.player1Id
    val playerName = PlayerMapper.lookup(playerId)
    val teamId = event._1.player1TeamId

    val season = event._1.season

    val primaryKey = buildPrimaryKey(playerId, players, season)

    val madeFT = PlayByPlayUtils.isMadeFreeThrow(event)
    val points = if (madeFT) 1 else 0

    val freeThrowAttempts = 1
    val freeThrowsMade = if (madeFT) 1 else 0


    val makeStats = UnitPlayerStats(
      primaryKey = primaryKey,
      season = season,
      dt = "",

      playerId = playerId,
      playerName = playerName,

      teamId = teamId,
      player1Id = players.head,
      player2Id = players(1),
      player3Id = players(2),
      player4Id = players(3),
      player5Id = players(4),

      player1Name = playerNames.head,
      player2Name = playerNames(1),
      player3Name = playerNames(2),
      player4Name = playerNames(3),
      player5Name = playerNames(4),

      points = points,
      freeThrowAttempts = freeThrowAttempts,
      freeThrowsMade = freeThrowsMade
    )

    val oppoPlayers = extractOppPlayers(event)
    val oppoTeam =  if (event._1.teamId1 == teamId) event._1.teamId2 else event._1.teamId1


    val offensePlayers = participatedInPossession(players, teamId, season, true)
    val defensePlayers = participatedInPossession(oppoPlayers, oppoTeam, season, false)

    makeStats +: (offensePlayers ++ defensePlayers)
  }

  def parseTurnoverEvent(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    if (PlayByPlayUtils.isSteal(event)) {
      parseTurnover(event) ++ parseSteal(event)
    } else {
      parseTurnover(event)
    }
  }

  def parseTurnover(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    val players = extractPlayers(event)
    val playerNames = getPlayerNames(players)

    val playerId = event._1.player1Id
    val playerName = PlayerMapper.lookup(playerId)
    val teamId = event._1.player1TeamId

    val season = event._1.season

    val primaryKey = buildPrimaryKey(playerId, players, season)

    val stat = UnitPlayerStats(
      primaryKey = primaryKey,
      season = season,
      dt = "",

      playerId = playerId,
      playerName = playerName,

      teamId = teamId,
      player1Id = players.head,
      player2Id = players(1),
      player3Id = players(2),
      player4Id = players(3),
      player5Id = players(4),

      player1Name = playerNames.head,
      player2Name = playerNames(1),
      player3Name = playerNames(2),
      player4Name = playerNames(3),
      player5Name = playerNames(4),

      turnovers = 1)

    val oppoPlayers = extractOppPlayers(event)
    val oppoTeam =  if (event._1.teamId1 == teamId) event._1.teamId2 else event._1.teamId1


    val offensePlayers = participatedInPossession(players, teamId, season, true)
    val defensePlayers = participatedInPossession(oppoPlayers, oppoTeam, season, false)

    stat +: (offensePlayers ++ defensePlayers)
  }

  def parseSteal(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    val players = extractOppPlayers(event)
    val playerNames = getPlayerNames(players)

    val playerId = event._1.player2Id
    val playerName = PlayerMapper.lookup(playerId)
    val teamId = event._1.player2TeamId

    val season = event._1.season

    val primaryKey = buildPrimaryKey(playerId, players, season)

    val stat = UnitPlayerStats(
      primaryKey = primaryKey,
      season = season,
      dt = "",

      playerId = playerId,
      playerName = playerName,

      teamId = teamId,
      player1Id = players.head,
      player2Id = players(1),
      player3Id = players(2),
      player4Id = players(3),
      player5Id = players(4),

      player1Name = playerNames.head,
      player2Name = playerNames(1),
      player3Name = playerNames(2),
      player4Name = playerNames(3),
      player5Name = playerNames(4),

      steals = 1)

    val oppoPlayers = extractOppPlayers(event)
    val oppoTeam =  if (event._1.teamId1 == teamId) event._1.teamId2 else event._1.teamId1


    val offensePlayers = participatedInPossession(players, teamId, season, false)
    val defensePlayers = participatedInPossession(oppoPlayers, oppoTeam, season, true)

    stat +: (offensePlayers ++ defensePlayers)
  }

  def parseRebound(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    val players = extractPlayers(event)
    val playerNames = getPlayerNames(players)

    val playerId = event._1.player1Id
    val playerName = PlayerMapper.lookup(playerId)
    val teamId = event._1.player1TeamId

    val season = event._1.season

    val primaryKey = buildPrimaryKey(playerId, players, season)

    val stat = UnitPlayerStats(
      primaryKey = primaryKey,
      season = season,
      dt = "",

      playerId = playerId,
      playerName = playerName,

      teamId = teamId,
      player1Id = players.head,
      player2Id = players(1),
      player3Id = players(2),
      player4Id = players(3),
      player5Id = players(4),

      player1Name = playerNames.head,
      player2Name = playerNames(1),
      player3Name = playerNames(2),
      player4Name = playerNames(3),
      player5Name = playerNames(4),

      rebounds = 1)

    val oppoPlayers = extractOppPlayers(event)
    val oppoTeam =  if (event._1.teamId1 == teamId) event._1.teamId2 else event._1.teamId1


    val offensePlayers = participatedInPossession(players, teamId, season, false)
    val defensePlayers = participatedInPossession(oppoPlayers, oppoTeam, season, true)

    stat +: (offensePlayers ++ defensePlayers)
  }

  //Need to figure out possessions and seconds for players who didn't participate

  def participatedInPossession(players: Seq[Integer], team: Integer, season: String, offense: Boolean): Seq[UnitPlayerStats] = {
    val names = getPlayerNames(players)
    players.map(p => {
      val playerName = PlayerMapper.lookup(p)
      val primaryKey = buildPrimaryKey(p, players, season)
      UnitPlayerStats(
        primaryKey = primaryKey,
        season = season,
        dt = "",

        playerId = p,
        playerName = playerName,

        teamId = team,
        player1Id = players.head,
        player2Id = players(1),
        player3Id = players(2),
        player4Id = players(3),
        player5Id = players(4),

        player1Name = names.head,
        player2Name = names(1),
        player3Name = names(2),
        player4Name = names(3),
        player5Name = names(4),
        offensivePossessions = if (offense) 1 else 0,
        defensivePossessions = if (offense) 0 else 1)
    })
  }

  def buildPrimaryKey(playerId: Integer, players: Seq[Integer], season: String): String = {
    s"${playerId}_${players.mkString("_")}_$season"
  }

}

case class UnitPlayerStats(primaryKey: String,
                           season: String,
                           dt: String,

                           playerId: jl.Integer,
                           playerName: String,

                           teamId: jl.Integer,
                           player1Id: jl.Integer,
                           player2Id: jl.Integer,
                           player3Id: jl.Integer,
                           player4Id: jl.Integer,
                           player5Id: jl.Integer,

                           player1Name: String,
                           player2Name: String,
                           player3Name: String,
                           player4Name: String,
                           player5Name: String,

                           points: jl.Integer = 0,

                           assists: jl.Integer = 0,

                           rebounds: jl.Integer = 0,

                           fouls: jl.Integer = 0,
                           foulsDrawn: jl.Integer = 0,

                           turnovers: jl.Integer = 0,

                           steals: jl.Integer = 0,
                           blocks: jl.Integer = 0,

                           fieldGoalAttempts: jl.Integer = 0,
                           fieldGoals: jl.Integer = 0,
                           assistedFieldGoals: jl.Integer = 0,

                           threePtAttempts: jl.Integer = 0,
                           threePtMade: jl.Integer = 0,
                           assisted3PtMade: jl.Integer = 0,

                           freeThrowAttempts: jl.Integer = 0,
                           freeThrowsMade: jl.Integer = 0,

                           offensivePossessions: jl.Integer = 0,
                           defensivePossessions: jl.Integer = 0,

                           seconds: jl.Integer = 0) {

  def +(other: UnitPlayerStats): UnitPlayerStats =
    UnitPlayerStats(
      primaryKey = primaryKey,
      season = season,
      dt = dt,

      playerId = playerId,
      playerName = playerName,

      teamId = teamId,
      player1Id = player1Id,
      player2Id = player2Id,
      player3Id = player3Id,
      player4Id = player4Id,
      player5Id = player5Id,

      player1Name = player1Name,
      player2Name = player2Name,
      player3Name = player3Name,
      player4Name = player4Name,
      player5Name = player5Name,

      points = points + other.points,

      assists = assists + other.assists,

      rebounds = rebounds + other.rebounds,

      fouls = fouls + other.fouls,
      foulsDrawn = foulsDrawn + other.foulsDrawn,

      turnovers = turnovers + other.turnovers,

      steals = steals + other.steals,

      blocks = blocks + other.blocks,

      fieldGoalAttempts = fieldGoalAttempts + other.fieldGoalAttempts,
      fieldGoals = fieldGoals + other.fieldGoals,
      assistedFieldGoals = assistedFieldGoals + other.assistedFieldGoals,

      threePtAttempts = threePtAttempts + other.threePtAttempts,
      threePtMade = threePtMade + other.threePtMade,
      assisted3PtMade = assisted3PtMade + other.assisted3PtMade,

      freeThrowAttempts = freeThrowAttempts + other.freeThrowAttempts,
      freeThrowsMade = freeThrowsMade + other.freeThrowsMade,

      offensivePossessions = offensivePossessions + other.offensivePossessions,
      defensivePossessions = defensivePossessions + other.defensivePossessions,

      seconds = seconds + other.seconds)

  def +~(other: UnitPlayerStats): UnitPlayerStats =
    UnitPlayerStats(
      primaryKey = primaryKey,
      season = season,
      dt = dt,

      playerId = playerId,
      playerName = playerName,

      teamId = teamId,
      player1Id = player1Id,
      player2Id = player2Id,
      player3Id = player3Id,
      player4Id = player4Id,
      player5Id = player5Id,

      player1Name = player1Name,
      player2Name = player2Name,
      player3Name = player3Name,
      player4Name = player4Name,
      player5Name = player5Name,

      points = points + other.points,

      assists = assists + other.assists,

      rebounds = rebounds + other.rebounds,

      fouls = fouls + other.fouls,
      foulsDrawn = foulsDrawn + other.foulsDrawn,

      turnovers = turnovers + other.turnovers,

      steals = steals + other.steals,

      blocks = blocks + other.blocks,

      fieldGoalAttempts = fieldGoalAttempts + other.fieldGoalAttempts,
      fieldGoals = fieldGoals + other.fieldGoals,
      assistedFieldGoals = assistedFieldGoals + other.assistedFieldGoals,

      threePtAttempts = threePtAttempts + other.threePtAttempts,
      threePtMade = threePtMade + other.threePtMade,
      assisted3PtMade = assisted3PtMade + other.assisted3PtMade,

      freeThrowAttempts = freeThrowAttempts + other.freeThrowAttempts,
      freeThrowsMade = freeThrowsMade + other.freeThrowsMade,

      offensivePossessions = (offensivePossessions + other.offensivePossessions) min 1,
      defensivePossessions = (defensivePossessions + other.defensivePossessions) min 1,

      seconds = seconds + other.seconds)

  def determinePossession: UnitPlayerStats =
    if (this.offensivePossessions > this.defensivePossessions) {
      this.copy(offensivePossessions = 1, defensivePossessions = 0)
    } else if (this.offensivePossessions < this.defensivePossessions) {
      this.copy(offensivePossessions = 0, defensivePossessions = 1)
    } else {
      this.copy(offensivePossessions = 0, defensivePossessions = 0)
    }
}