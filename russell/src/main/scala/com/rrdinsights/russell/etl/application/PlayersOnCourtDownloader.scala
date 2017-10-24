package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.PlayersOnCourt
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.AdvancedBoxScoreEndpoint
import com.rrdinsights.scalabrine.parameters.{EndRangeParameter, GameIdParameter, RangeTypeParameter, StartRangeParameter}

object PlayersOnCourtDownloader {

  def downloadPlayersOnCourt(gameId: String, time: Int): Seq[(Integer, Integer)] = {
    val gameIdParameter = GameIdParameter.newParameterValue(gameId)
    val rangeType = RangeTypeParameter.newParameterValue("2")
    val startRange = StartRangeParameter.newParameterValue((time - 1).toString)
    val endRange = EndRangeParameter.newParameterValue(time.toString)

    val endpoint = AdvancedBoxScoreEndpoint(
      gameId = gameIdParameter,
      rangeType = rangeType,
      startRange = startRange,
      endRange = endRange)

    ScalabrineClient
      .getAdvancedBoxScore(endpoint)
      .boxScoreAdvanced
      .playerStats
      .map(v => (v.playerId, v.teamId))
      .sortBy(v => (v._2, v._1))
  }

  def timeFromStartOfGame(period: Int, minutesRemaining: Int, secondsRemaining: Int): Int = {
    val previousPeriods = periodToMinutesPlayed(period) * 60
    val minutesElapsedThisPeriod = (minutesInPeriod(period) - minutesRemaining - 1) * 60
    val secondsElapsed = 60 - secondsRemaining

    previousPeriods + minutesElapsedThisPeriod + secondsElapsed
  }

  def timeFromStartOfGameAtPeriod(period: Int): Int =
    timeFromStartOfGame(period, minutesInPeriod(period), 50)

  private[application] def periodToMinutesPlayed(period: Int): Int =
    if (period > 4) {
      (4 * 12) + ((period - 5) * 5)
    } else {
      (period - 1) * 12
    }

  private[application] def minutesInPeriod(period: Int): Int =
    if (period > 4) {
      5
    } else {
      12
    }

  def writePlayersOnCourt(players: Seq[PlayersOnCourt]): Unit = {
    MySqlClient.createTable(NBATables.players_on_court)
    MySqlClient.insertInto(NBATables.players_on_court, players)
  }
}