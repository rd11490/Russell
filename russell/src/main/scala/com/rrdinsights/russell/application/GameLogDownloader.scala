package com.rrdinsights.russell.application

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.GameRecord
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.TeamGameLogEndpoint
import com.rrdinsights.scalabrine.models.GameLog
import com.rrdinsights.scalabrine.parameters.{ParameterValue, SeasonParameter, TeamIdParameter}

object GameLogDownloader {

  def downloadAndWriteAllGameLogs(season: String, dt: String): Unit = {
    val gameLog = downloadAllGameLogs(season)
    writeGameLogs(gameLog, season, dt)
  }

  private def downloadTeamGameLog(seasonParameter: ParameterValue, teamId: ParameterValue): Seq[GameLog] = {
    val gameLogEndpoint = TeamGameLogEndpoint(teamId, seasonParameter)
    ScalabrineClient.getTeamGameLog(gameLogEndpoint).teamGameLog.games
    try {
      ScalabrineClient.getTeamGameLog(gameLogEndpoint).teamGameLog.games
    } catch {
      case e: Throwable =>
        println("Failed to Download!")
        println(seasonParameter.toUrl)
        println(e)
        Seq.empty
    }
  }

  private def downloadAllGameLogs(season: String): Seq[GameLog] = {
    val seasonParameter = SeasonParameter.newParameterValue(season)
    TeamIdParameter.TeamIds
      .flatMap(v => {
        Thread.sleep(1000)
        downloadTeamGameLog(seasonParameter, v)
      })
  }

  private def writeGameLogs(gameLogs: Seq[GameLog], season: String, dt: String): Unit = {
    MySqlClient.createTable(NBATables.game_record)
    val gameRecords = gameLogs.map(GameRecord.apply(_, season, dt))
    MySqlClient.insertInto(NBATables.game_record, gameRecords)
  }

  def readGameLogs(season: String): Seq[GameRecord] =
    MySqlClient.selectFrom[GameRecord](
      NBATables.game_record,
      GameRecord.apply,
      s"Season = '$season'")


}
