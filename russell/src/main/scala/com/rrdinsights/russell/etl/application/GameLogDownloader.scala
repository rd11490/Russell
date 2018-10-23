package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.GameRecord
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.TeamGameLogEndpoint
import com.rrdinsights.scalabrine.models.GameLog
import com.rrdinsights.scalabrine.parameters.{ParameterValue, SeasonParameter, SeasonTypeParameter, TeamIdParameter}

object GameLogDownloader {

  def downloadAndWriteAllGameLogs(season: String, dt: String, seasonType: String): Unit = {
    val gameLog = downloadAllGameLogs(season, seasonType)
    writeGameLogs(gameLog, season, dt, seasonType)
  }

  private def downloadTeamGameLog(seasonParameter: ParameterValue, teamId: ParameterValue, seasonType: ParameterValue): Seq[GameLog] = {
    try {
      val gameLogEndpoint = TeamGameLogEndpoint(teamId = teamId, season = seasonParameter, seasonTypeParameter = seasonType)
      ScalabrineClient.getTeamGameLog(gameLogEndpoint).teamGameLog.games
    } catch {
      case e: Throwable =>
        println("Failed to Download!")
        println(seasonParameter.toUrl)
        println(e)
        Seq.empty
    }
  }

  private def downloadAllGameLogs(season: String, seasonType: String): Seq[GameLog] = {
    val seasonParameter = SeasonParameter.newParameterValue(season)
    val seasonTypeParameter = SeasonTypeParameter.newParameterValue(seasonType)
    TeamIdParameter.TeamIds
      .flatMap(v => {
        println(s"Downloading Game Logs for: $v")
        Thread.sleep(1000)
        downloadTeamGameLog(seasonParameter, v, seasonTypeParameter)
      })
  }

  private def writeGameLogs(gameLogs: Seq[GameLog], season: String, dt: String, seasonType: String): Unit = {
    MySqlClient.createTable(NBATables.game_record)
    val gameRecords = gameLogs.map(GameRecord.apply(_, season, dt, seasonType))
    MySqlClient.insertInto(NBATables.game_record, gameRecords)
  }

  def readGameLogs(season: String, seasonType: String): Seq[GameRecord] =
    MySqlClient.selectFrom[GameRecord](
      NBATables.game_record,
      GameRecord.apply,
      s"Season = '$season' and SeasonType = '$seasonType'")


}
