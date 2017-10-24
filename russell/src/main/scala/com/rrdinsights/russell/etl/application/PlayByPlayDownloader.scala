package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{GameRecord, RawPlayByPlayEvent}
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.PlayByPlayEndpoint
import com.rrdinsights.scalabrine.models.PlayByPlayEvent
import com.rrdinsights.scalabrine.parameters.{GameIdParameter, ParameterValue}

object PlayByPlayDownloader {

  def downloadAndWriteAllPlayByPlay(gameLogs: Seq[GameRecord], season: String, dt: String): Unit = {
    val playByPlay = downloadAllPlayByPlay(gameLogs)
    writePlayByPlay(playByPlay, season, dt)
  }

  private def downloadAllPlayByPlay(gameLogs: Seq[GameRecord]): Seq[PlayByPlayEvent] = {
    gameLogs
      .map(_.gameId)
      .distinct
      .map(GameIdParameter.newParameterValue)
      .flatMap(v => {
        Thread.sleep(1000)
        downloadGamePlayByPlay(v)
      })
  }

  private def downloadGamePlayByPlay(gameIdParamter: ParameterValue): Seq[PlayByPlayEvent] = {
    val endpoint = PlayByPlayEndpoint(gameIdParamter)
    try {
      ScalabrineClient.getPlayByPlay(endpoint).playByPlay.events
    } catch {
      case e: Throwable =>
        println("Failed to Download!")
        println(gameIdParamter.toUrl)
        println(e)
        Seq.empty
    }
  }

  private def writePlayByPlay(events: Seq[PlayByPlayEvent], season: String, dt: String): Unit = {
    MySqlClient.createTable(NBATables.raw_play_by_play)
    val gameRecords = events.map(RawPlayByPlayEvent.apply(_, season, dt))
    MySqlClient.insertInto(NBATables.raw_play_by_play, gameRecords)
  }

  def readPlayByPlay(where: String*): Seq[RawPlayByPlayEvent] = {
    MySqlClient.selectFrom[RawPlayByPlayEvent](
      NBATables.raw_play_by_play,
      RawPlayByPlayEvent.apply,
      where:_ *)
  }
}