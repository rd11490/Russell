package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel._
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.AdvancedBoxScoreEndpoint
import com.rrdinsights.scalabrine.models._
import com.rrdinsights.scalabrine.parameters.{GameIdParameter, ParameterValue}

object AdvancedBoxScoreDownloader {

  def downloadAndWriteAllAdvancedBoxScores(gameLogs: Seq[GameRecord], season: String, dt: String): Unit = {
    val advancedBoxScore = downloadAllAdvancedBoxScores(gameLogs)
    writePlayerStats(advancedBoxScore.flatMap(_.playerStats), season, dt)
    writeTeamStats(advancedBoxScore.flatMap(_.teamStats), season, dt)
  }


  private def downloadAllAdvancedBoxScores(gameLogs: Seq[GameRecord]): Seq[BoxScoreAdvanced] = {
    gameLogs
      .map(_.gameId)
      .distinct
      .map(GameIdParameter.newParameterValue)
      .flatMap(v => {
        Thread.sleep(1000)
        downloadAdvancedBoxScore(v)
      })
  }

  private def downloadAdvancedBoxScore(gameIdParamter: ParameterValue): Option[BoxScoreAdvanced] = {
    val endpoint = AdvancedBoxScoreEndpoint(gameIdParamter)
    try {
      Some(ScalabrineClient.getAdvancedBoxScore(endpoint).boxScoreAdvanced)
    } catch {
      case e: Throwable =>
        println("Failed to Download!")
        println(gameIdParamter.toUrl)
        println(e)
        None
    }
  }

  private def writeTeamStats(teamStats: Seq[TeamStats], season: String, dt: String): Unit = {
    MySqlClient.createTable(NBATables.raw_team_box_score_advanced)
    val teamStatsAdvanced = teamStats.map(RawTeamBoxScoreAdvanced(_, season, dt))
    MySqlClient.insertInto(NBATables.raw_team_box_score_advanced, teamStatsAdvanced)
  }

  private def writePlayerStats(players: Seq[PlayerStats], season: String, dt: String): Unit = {
    MySqlClient.createTable(NBATables.raw_player_box_score_advanced)
    val playersAdvanced = players.map(RawPlayerBoxScoreAdvanced(_, season, dt))
    MySqlClient.insertInto(NBATables.raw_player_box_score_advanced, playersAdvanced)
  }


}
