package com.rrdinsights.russell.investigation.playbyplay

import com.rrdinsights.russell.etl.application.ShotChartDownloader
import com.rrdinsights.russell.investigation.TeamMapper
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{PlayByPlayEventMessageType, PlayByPlayWithLineup}
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.TimeUtils
import com.rrdinsights.scalabrine.utils.CSVWriter

object PlayByPlayExplore {
  /**
    * This object is just for playing with play by play data to help with production tasks
    *
    */
  def main(strings: Array[String]): Unit = {
    val playByPlay = MySqlClient.selectFrom(NBATables.play_by_play_with_lineup, PlayByPlayWithLineup.apply, "season = '2017-18'")

    val gameId = playByPlay.head.gameId

    playByPlay
      .filter(_.gameId == gameId)
      .groupBy(v => (v.gameId, v.period))
      .flatMap(v => rollPlayByPlay(v._2))
      .filter(filterPossibleTransition)
      .foreach(println)

  }

  private def filterPossibleTransition(roll: PlayRoll): Boolean = {
    val lastPlayType = PlayByPlayEventMessageType.valueOf(roll.lastPlayType)
    val playType = PlayByPlayEventMessageType.valueOf(roll.lastPlayType)

    (roll.timeSeconds <= 7) &&
      (lastPlayType == PlayByPlayEventMessageType.Turnover ||
        lastPlayType == PlayByPlayEventMessageType.Rebound ||
        lastPlayType == PlayByPlayEventMessageType.Make) &&
      (playType == PlayByPlayEventMessageType.Make ||
        playType == PlayByPlayEventMessageType.Miss ||
        playType == PlayByPlayEventMessageType.Turnover ||
        playType == PlayByPlayEventMessageType.Foul)
  }


  private def rollPlayByPlay(plays: Seq[PlayByPlayWithLineup]): Seq[PlayRoll] = {
    val sortedPlays = plays.sortBy(_.eventNumber)
    val lastPlays = sortedPlays.dropRight(1)
    val currentPlays = sortedPlays.tail
    (lastPlays zip currentPlays).map(v => toPlayRoll(v._1, v._2))
  }

  private def toPlayRoll(left: PlayByPlayWithLineup, right: PlayByPlayWithLineup): PlayRoll = {
    PlayRoll(left.gameId, right.eventNumber, left.playType, right.playType, timeBetween(left.period, left.pcTimeString, right.pcTimeString), left.period, left.pcTimeString, right.pcTimeString)
  }

  private def timeBetween(period: Integer, timeLast: String, timeCurrent: String): Integer =
    TimeUtils.convertTimeStringToTime(period, timeCurrent) - TimeUtils.convertTimeStringToTime(period, timeLast)

}

private final case class PlayRoll(gameId: String, eventNumber: Integer, lastPlayType: String, playType: String, timeSeconds: Integer, period: Integer, timeLast: String, time: String)