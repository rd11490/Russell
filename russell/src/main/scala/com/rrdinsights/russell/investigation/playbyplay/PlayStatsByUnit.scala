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
      .map(v => v.copy(seconds = possession._2, possessions = 1))
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
      Seq(
        parseMake(event),
        parseAssist(event))
    } else {
      Seq(parseMake(event))
    }

  }

  def parseMake(event: (PlayByPlayWithLineup, Option[ScoredShot])): UnitPlayerStats = {
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

    UnitPlayerStats(
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
  }

  def parseAssist(event: (PlayByPlayWithLineup, Option[ScoredShot])): UnitPlayerStats = {
    val players = extractPlayers(event)
    val playerNames = getPlayerNames(players)

    val playerId = event._1.player2Id
    val playerName = PlayerMapper.lookup(playerId)
    val teamId = event._1.player2TeamId

    val season = event._1.season

    val primaryKey = buildPrimaryKey(playerId, players, season)

    UnitPlayerStats(
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
  }


  def parseMissEvent(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    if (PlayByPlayUtils.isBlock(event)) {
      Seq(
        parseMiss(event),
        parseBlock(event))
    } else {
      Seq(parseMiss(event))
    }
  }

  def parseMiss(event: (PlayByPlayWithLineup, Option[ScoredShot])): UnitPlayerStats = {
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

    UnitPlayerStats(
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
  }

  def parseBlock(event: (PlayByPlayWithLineup, Option[ScoredShot])): UnitPlayerStats = {
    val players = extractOppPlayers(event)
    val playerNames = getPlayerNames(players)

    val playerId = event._1.player3Id
    val playerName = PlayerMapper.lookup(playerId)
    val teamId = event._1.player2TeamId

    val season = event._1.season

    val primaryKey = buildPrimaryKey(playerId, players, season)

    UnitPlayerStats(
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
  }

  def parseFoulEvent(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    Seq(
      parseFoulCommited(event),
      parseFoulDrawn(event))
  }

  def parseFoulCommited(event: (PlayByPlayWithLineup, Option[ScoredShot])): UnitPlayerStats = {
    val players = extractPlayers(event)
    val playerNames = getPlayerNames(players)

    val playerId = event._1.player1Id
    val playerName = PlayerMapper.lookup(playerId)
    val teamId = event._1.player1TeamId

    val season = event._1.season

    val primaryKey = buildPrimaryKey(playerId, players, season)

    UnitPlayerStats(
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
  }

  def parseFoulDrawn(event: (PlayByPlayWithLineup, Option[ScoredShot])): UnitPlayerStats = {
    val players = extractOppPlayers(event)
    val playerNames = getPlayerNames(players)

    val playerId = event._1.player2Id
    val playerName = PlayerMapper.lookup(playerId)
    val teamId = event._1.player2TeamId

    val season = event._1.season

    val primaryKey = buildPrimaryKey(playerId, players, season)

    UnitPlayerStats(
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


    Seq(UnitPlayerStats(
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
    ))
  }

  def parseTurnoverEvent(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    if (PlayByPlayUtils.isSteal(event)) {
      Seq(
        parseTurnover(event),
        parseSteal(event))
    } else {
      Seq(parseTurnover(event))
    }
  }

  def parseTurnover(event: (PlayByPlayWithLineup, Option[ScoredShot])): UnitPlayerStats = {
    val players = extractPlayers(event)
    val playerNames = getPlayerNames(players)

    val playerId = event._1.player1Id
    val playerName = PlayerMapper.lookup(playerId)
    val teamId = event._1.player1TeamId

    val season = event._1.season

    val primaryKey = buildPrimaryKey(playerId, players, season)

    UnitPlayerStats(
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
  }

  def parseSteal(event: (PlayByPlayWithLineup, Option[ScoredShot])): UnitPlayerStats = {
    val players = extractOppPlayers(event)
    val playerNames = getPlayerNames(players)

    val playerId = event._1.player2Id
    val playerName = PlayerMapper.lookup(playerId)
    val teamId = event._1.player2TeamId

    val season = event._1.season

    val primaryKey = buildPrimaryKey(playerId, players, season)

    UnitPlayerStats(
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
  }

  def parseRebound(event: (PlayByPlayWithLineup, Option[ScoredShot])): Seq[UnitPlayerStats] = {
    val players = extractPlayers(event)
    val playerNames = getPlayerNames(players)

    val playerId = event._1.player1Id
    val playerName = PlayerMapper.lookup(playerId)
    val teamId = event._1.player1TeamId

    val season = event._1.season

    val primaryKey = buildPrimaryKey(playerId, players, season)

    Seq(UnitPlayerStats(
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

      rebounds = 1))
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

                           possessions: jl.Integer = 0,
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

      possessions = possessions + other.possessions,
      seconds = seconds + other.seconds)
}