package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel._
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.BoxScoreEndpoint
import com.rrdinsights.scalabrine.models._
import com.rrdinsights.scalabrine.parameters.{ParameterValue, GameIdParameter}

object BoxScoreSummaryDownloader {

  def downloadAndWriteAllBoxScoreSummaries(gameLogs: Seq[GameRecord], dt: String, season: Option[String]): Unit = {
    downloadAlBoxScoreSummaries(gameLogs)
      .foreach(v => writeBoxScoreSummaryComponents(v._2, v._1, dt, season))
  }

  def writeBoxScoreSummaryComponents(summary: BoxScoreSummary, gameId: String, dt: String, season: Option[String]): Unit = {
    summary.gameSummary.foreach(writeGameSummary(_, dt, season))
    summary.gameInfo.foreach(writeGameInfo(_, gameId, dt, season))
    writeInactivePlayres(summary.inactivePlayers, gameId, dt, season)
    writeOfficials(summary.officials, gameId, dt, season)
    val scoreLines = Seq(summary.homeStats.scoreLine, summary.awayStats.scoreLine).flatten
    writeScoreLines(scoreLines, dt, season)
    val otherStats = Seq(summary.homeStats.otherStats, summary.awayStats.otherStats).flatten
    writeOtherStats(otherStats, gameId, dt, season)
  }


  private def downloadAlBoxScoreSummaries(gameLogs: Seq[GameRecord]): Seq[(String, BoxScoreSummary)] = {
    gameLogs
      .map(_.gameId)
      .distinct
      .map(GameIdParameter.newParameterValue)
      .flatMap(v => {
        Thread.sleep(1000)
        downloadBoxScoreSummary(v).map(s => (v.value, s))
      })
  }

  private def downloadBoxScoreSummary(gameIdParamter: ParameterValue): Option[BoxScoreSummary] = {
    val endpoint = BoxScoreEndpoint(gameIdParamter)
    try {
      Some(ScalabrineClient.getBoxScoreSummary(endpoint).boxScoreSummary)
    } catch {
      case e: Throwable =>
        println("Failed to Download!")
        println(gameIdParamter.toUrl)
        println(e)
        None
    }
  }

  private def writeGameSummary(summary: GameSummary, dt: String, season: Option[String]): Unit = {
    val rawGameSummary = RawGameSummary(summary, dt, season)
    MySqlClient.createTable(NBATables.raw_game_summary)
    MySqlClient.insertInto(NBATables.raw_game_summary, Seq(rawGameSummary))
  }

  private def writeGameInfo(info: GameInfo, gameId: String, dt: String, season: Option[String]): Unit = {
    val rawGameInfo = RawGameInfo(info, gameId, dt, season)
    MySqlClient.createTable(NBATables.raw_game_info)
    MySqlClient.insertInto(NBATables.raw_game_info, Seq(rawGameInfo))
  }

  private def writeScoreLines(scoreLines: Seq[ScoreLine], dt: String, season: Option[String]): Unit = {
    val rawScoreLines = scoreLines.map(RawGameScoreLine(_, dt, season))
    MySqlClient.createTable(NBATables.raw_game_score_line)
    MySqlClient.insertInto(NBATables.raw_game_score_line, rawScoreLines)
  }

  private def writeOtherStats(otherStats: Seq[OtherStats], gameId: String, dt: String, season: Option[String]): Unit = {
    val rawOtherStats = otherStats.map(RawOtherStats(_, gameId, dt, season))
    MySqlClient.createTable(NBATables.raw_game_other_stats)
    MySqlClient.insertInto(NBATables.raw_game_other_stats, rawOtherStats)
  }

  private def writeOfficials(officials: Seq[Officials], gameId: String, dt: String, season: Option[String]): Unit = {
    val gameOfficials = officials.map(GameOfficial(_, gameId, dt, season))
    MySqlClient.createTable(NBATables.game_officials)
    MySqlClient.insertInto(NBATables.game_officials, gameOfficials)
  }

  private def writeInactivePlayres(inactivePlayers: Seq[InactivePlayers], gameId: String, dt: String, season: Option[String]): Unit = {
    val rawInactivePlayers = inactivePlayers.map(v => RawInactivePlayers(v, gameId, dt, season))
    MySqlClient.createTable(NBATables.raw_game_inactive_players)
    MySqlClient.insertInto(NBATables.raw_game_inactive_players, rawInactivePlayers)
  }
}
