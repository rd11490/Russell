package com.rrdinsights.russell.etl.driver

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.rrdinsights.russell.etl.application.{PlayersOnCourtDownloader, ShotChartDownloader}
import com.rrdinsights.russell.storage.datamodel.{DataModelUtils, PlayersOnCourt, RawShotData}

object PlayerOnCourtDriver {

  private val Formatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

  def main(strings: Array[String]): Unit = {
    val dt = LocalDateTime.now().format(Formatter)
    downloadAndWritePlayerOnCourtForShots(dt)
  }

  private def downloadAndWritePlayerOnCourtForShots(dt: String): Unit = {
    val rawShotData = ShotChartDownloader.readShotData(s"season >= 2013")
    val playersOnCourt = rawShotData.flatMap(downloadPlayersOnCourtDuringShot(_, dt))
    PlayersOnCourtDownloader.writePlayersOnCourt(playersOnCourt)
  }

  def downloadPlayersOnCourtDuringShot(rawShotData: RawShotData, dt: String): Option[PlayersOnCourt] = {
    Thread.sleep(500)
    val gameId = rawShotData.gameId
    val time = PlayersOnCourtDownloader.timeFromStartOfGame(rawShotData.period, rawShotData.minutesRemaining, rawShotData.secondsRemaining) * 10

    val players = PlayersOnCourtDownloader.downloadPlayersOnCourt(gameId, time)
    if (players.size == 10 && players.slice(0, 4).map(_._2).distinct.size == 1 && players.slice(5, 9).map(_._2).distinct.size == 1) {
      val primaryKey = s"${rawShotData.gameId}_${rawShotData.gameEventId}"
      Some(PlayersOnCourt(
        rawShotData.primaryKey,
        rawShotData.gameId,
        rawShotData.gameEventId,
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
        DataModelUtils.gameIdToSeason(rawShotData.gameId)))
    }
    else {
      println(s"${rawShotData.gameId} - ${rawShotData.gameEventId}")
      println(s"${rawShotData.period} ${rawShotData.minutesRemaining} - ${rawShotData.secondsRemaining}")
      println(s"$time")
      println(s"${players.size}")
      println(s"${players.groupBy(_._2).map(v => s"${v._1} - ${v._2.size}").mkString(" | ")}")
      None
    }
  }
}