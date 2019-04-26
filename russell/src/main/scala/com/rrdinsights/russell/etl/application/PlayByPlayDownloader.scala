package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.investigation.GameDateMapper
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{GameRecord, RawPlayByPlayEvent}
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.PlayByPlayEndpoint
import com.rrdinsights.scalabrine.models.PlayByPlayEvent
import com.rrdinsights.scalabrine.parameters.{GameIdParameter, ParameterValue}

object PlayByPlayDownloader {

  def downloadAndWriteAllPlayByPlay(gameLogs: Seq[GameRecord], season: String, dt: String, seasonType: String, force: Boolean = false): Unit = {
    val playByPlay = downloadAllPlayByPlay(gameLogs, force)
    writePlayByPlay(playByPlay, season, dt, seasonType)
  }

  private def downloadAllPlayByPlay(gameLogs: Seq[GameRecord], force: Boolean): Seq[PlayByPlayEvent] = {
    gameLogs
      .map(_.gameId)
      .distinct
      .filter(v => force || GameDateMapper.gameDate(v).isEmpty)
      .map(GameIdParameter.newParameterValue)
      .flatMap(v => {
        println(v)
        Thread.sleep(1000)
        downloadGamePlayByPlay(v)
      })
  }

  private def downloadGamePlayByPlay(gameIdParamter: ParameterValue): Seq[PlayByPlayEvent] = {
    val endpoint = PlayByPlayEndpoint(gameId = gameIdParamter)
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

  private def writePlayByPlay(events: Seq[PlayByPlayEvent], season: String, dt: String, seasonType: String): Unit = {
    MySqlClient.createTable(NBATables.raw_play_by_play)
    val gameRecords = events.map(RawPlayByPlayEvent.apply(_, season, dt, seasonType))
    MySqlClient.insertInto(NBATables.raw_play_by_play, gameRecords)
  }

  def readPlayByPlay(where: String*): Seq[RawPlayByPlayEvent] = {
    MySqlClient.selectFrom[RawPlayByPlayEvent](
      NBATables.raw_play_by_play,
      RawPlayByPlayEvent.apply,
      where:_ *)
  }
}