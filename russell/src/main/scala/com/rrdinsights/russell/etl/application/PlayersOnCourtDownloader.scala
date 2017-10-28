package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{DataModelUtils, PlayersOnCourt, RawPlayByPlayEvent}
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.TimeUtils
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.AdvancedBoxScoreEndpoint
import com.rrdinsights.scalabrine.parameters.{EndRangeParameter, GameIdParameter, RangeTypeParameter, StartRangeParameter}

object PlayersOnCourtDownloader {

  def downloadPlayersOnCourtAtEvent(playByPlay: RawPlayByPlayEvent, dt: String): Option[PlayersOnCourt] = {
    val time = TimeUtils.convertTimeStringToTime(playByPlay.period.intValue, playByPlay.pcTimeString) * 10
    val players = PlayersOnCourtDownloader.downloadPlayersOnCourt(playByPlay.gameId, time)
    if (players.size == 10 && players.slice(0, 4).map(_._2).distinct.size == 1 && players.slice(5, 9).map(_._2).distinct.size == 1) {
      val primaryKey = s"${playByPlay.gameId}_${playByPlay.period}"
      Some(PlayersOnCourt(
        primaryKey,
        playByPlay.gameId,
        null,
        playByPlay.period,
        players.head._2,
        players.head._1,
        players(1)._1,
        players(2)._1,
        players(3)._1,
        players(4)._1,
        players(5)._2,
        players(5)._1,
        players(6)._1,
        players(7)._1,
        players(8)._1,
        players(9)._1,
        dt,
        DataModelUtils.gameIdToSeason(playByPlay.gameId)))

    }
    else {
      println(s"FAILURE")
      println(s"${playByPlay.gameId}-${playByPlay.period}")
      println(s"$time")
      println(s"${players.size}")
      println(s"${players.groupBy(_._2).map(v => s"${v._1} - ${v._2.size}").mkString(" | ")}")
      None
    }
  }

  def downloadPlayersOnCourtAtStartOfPeriod(gameId: String, period: Int, dt: String): Option[PlayersOnCourt] = {
    val time = TimeUtils.timeFromStartOfGameAtPeriod(period) * 10
    val players = PlayersOnCourtDownloader.downloadPlayersOnCourt(gameId, time)
    if (players.size == 10 && players.slice(0, 4).map(_._2).distinct.size == 1 && players.slice(5, 9).map(_._2).distinct.size == 1) {
      val primaryKey = s"${gameId}_$period"
      Some(PlayersOnCourt(
        primaryKey,
        gameId,
        null,
        period,
        players.head._2,
        players.head._1,
        players(1)._1,
        players(2)._1,
        players(3)._1,
        players(4)._1,
        players(5)._2,
        players(5)._1,
        players(6)._1,
        players(7)._1,
        players(8)._1,
        players(9)._1,
        dt,
        DataModelUtils.gameIdToSeason(gameId)))

    }
    else {
      println(s"FAILURE")
      println(s"$gameId-$period")
      println(s"$time")
      println(s"${players.size}")
      println(s"${players.groupBy(_._2).map(v => s"${v._1} - ${v._2.size}").mkString(" | ")}")
      None
    }
  }

  def downloadPlayersOnCourt(gameId: String, time: Int): Seq[(Integer, Integer)] = {
    val gameIdParameter = GameIdParameter.newParameterValue(gameId)
    val rangeType = RangeTypeParameter.newParameterValue("2")
    val startRange = StartRangeParameter.newParameterValue((time + 10).toString)
    val endRange = EndRangeParameter.newParameterValue((time + 240).toString)

    val endpoint = AdvancedBoxScoreEndpoint(
      gameId = gameIdParameter,
      rangeType = rangeType,
      startRange = startRange,
      endRange = endRange)

    println(endpoint.url)

    Thread.sleep(2000)
    ScalabrineClient
      .getAdvancedBoxScore(endpoint)
      .boxScoreAdvanced
      .playerStats
      .map(v => (v.playerId, v.teamId))
      .sortBy(v => (v._2, v._1))
  }

  def writePlayersOnCourt(players: Seq[PlayersOnCourt]): Unit = {
    MySqlClient.createTable(NBATables.players_on_court)
    MySqlClient.insertInto(NBATables.players_on_court, players)
  }

  def writePlayersOnCourtAtPeriod(players: Seq[PlayersOnCourt]): Unit = {
    MySqlClient.createTable(NBATables.players_on_court_at_period)
    MySqlClient.insertInto(NBATables.players_on_court_at_period, players)
  }

  def readPlayersOnCourtAtPeriod(where: String*): Seq[PlayersOnCourt] = {
    MySqlClient.selectFrom[PlayersOnCourt](
      NBATables.players_on_court_at_period,
      PlayersOnCourt.apply,
      where:_ *)
  }
}