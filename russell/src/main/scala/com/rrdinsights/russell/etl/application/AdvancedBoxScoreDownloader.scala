package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel._
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.AdvancedBoxScoreEndpoint
import com.rrdinsights.scalabrine.models._
import com.rrdinsights.scalabrine.parameters.{GameIdParameter, ParameterValue, SeasonParameter, SeasonTypeParameter}

object AdvancedBoxScoreDownloader {

  def downloadAndWriteAllAdvancedBoxScores(gameLogs: Seq[GameRecord], season: String, dt: String, seasonType: String): Unit = {
    val advancedBoxScore = downloadAllAdvancedBoxScores(gameLogs)
    writePlayerStats(advancedBoxScore.flatMap(_.playerStats), season, dt, seasonType)
    writeTeamStats(advancedBoxScore.flatMap(_.teamStats), season, dt, seasonType)
  }


  private def downloadAllAdvancedBoxScores(gameLogs: Seq[GameRecord]): Seq[BoxScoreAdvanced] = {
    gameLogs
      .map(v => (v.gameId, v.season, v.seasonType))
      .distinct
      .map(v => (GameIdParameter.newParameterValue(v._1), SeasonParameter.newParameterValue(v._2), SeasonTypeParameter.newParameterValue(v._3)))
      .flatMap(v => {
        println(v)
        Thread.sleep(1000)
        downloadAdvancedBoxScore(v._1, v._2, v._3)
      })
  }

  private def downloadAdvancedBoxScore(gameIdParamter: ParameterValue, season: ParameterValue, seasonType: ParameterValue): Option[BoxScoreAdvanced] = {
    val endpoint = AdvancedBoxScoreEndpoint(gameIdParamter, season, seasonType)
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

  private def writeTeamStats(teamStats: Seq[TeamStats], season: String, dt: String, seasonType: String): Unit = {
    MySqlClient.createTable(NBATables.raw_team_box_score_advanced)
    val teamStatsAdvanced = teamStats.map(RawTeamBoxScoreAdvanced(_, season, dt, seasonType))
    MySqlClient.insertInto(NBATables.raw_team_box_score_advanced, teamStatsAdvanced)
  }

  private def writePlayerStats(players: Seq[PlayerStats], season: String, dt: String, seasonType: String): Unit = {
    MySqlClient.createTable(NBATables.raw_player_box_score_advanced)
    val playersAdvanced = players.map(RawPlayerBoxScoreAdvanced(_, season, dt, seasonType))
    MySqlClient.insertInto(NBATables.raw_player_box_score_advanced, playersAdvanced)
  }

  def readPlayerStats(whereClauses: String*): Seq[RawPlayerBoxScoreAdvanced] =
    MySqlClient.selectFrom(NBATables.raw_player_box_score_advanced, RawPlayerBoxScoreAdvanced.apply, whereClauses:_*)

}