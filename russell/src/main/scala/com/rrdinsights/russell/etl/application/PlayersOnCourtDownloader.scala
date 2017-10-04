package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.storage.datamodel.RawShotData
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.AdvancedBoxScoreEndpoint
import com.rrdinsights.scalabrine.parameters.{GameIdParameter, RangeTypeParameter, StartRangeParameter}

object PlayersOnCourtDownloader {

  def downloadPlayersOnCourt(gameId: String, time: Int): Seq[(String, String)] = {
    val gameIdParameter = GameIdParameter.newParameterValue(gameId)
    val rangeType = RangeTypeParameter.newParameterValue("2")
    val startRange = StartRangeParameter.newParameterValue((time - 1).toString)
    val endRange = StartRangeParameter.newParameterValue(time.toString)

    val endpoint = AdvancedBoxScoreEndpoint(
      gameId = gameIdParameter,
      rangeType = rangeType,
      startRange = startRange,
      endRange = endRange)

    ScalabrineClient
      .getAdvancedBoxScore(endpoint)
      .boxScoreAdvanced
      .playerStats
      .map(v => (v.playerId.toString, v.teamId.toString))
  }

  def downloadPlayersOnCourtDurringShot(rawShotData: RawShotData): Unit = {
    val gameId = rawShotData.gameId
    val time = timeFromStartOfGame(rawShotData) * 10

    downloadPlayersOnCourt(gameId, time)
  }

  private[application] def timeFromStartOfGame(rawShotData: RawShotData): Int = {
    val previousPeriods = periodToMinutesPlayed(rawShotData.period) * 60
    val minutesElapsedThisPeriod = (minutesInPeriod(rawShotData.period) - rawShotData.minutesRemaining - 1) * 60
    val secondsElapsed = 60 - rawShotData.secondsRemaining

    previousPeriods + minutesElapsedThisPeriod + secondsElapsed
  }

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
}