package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.investigation.GameDateMapper
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel._
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.BoxScoreEndpoint
import com.rrdinsights.scalabrine.models._
import com.rrdinsights.scalabrine.parameters.{GameIdParameter, ParameterValue}

object BoxScoreSummaryDownloader {

  def downloadAndWriteAllBoxScoreSummaries(gameLogs: Seq[GameRecord], dt: String, season: Option[String], seasonType: String, force: Boolean = false): Unit = {
    downloadAlBoxScoreSummaries(gameLogs, force)
      .foreach(v => writeBoxScoreSummaryComponents(v._2, v._1, dt, season, seasonType))
  }

  def writeBoxScoreSummaryComponents(summary: BoxScoreSummary, gameId: String, dt: String, season: Option[String], seasonType: String): Unit = {
    summary.gameSummary.foreach(writeGameSummary(_, dt, season, seasonType))
    summary.gameInfo.foreach(writeGameInfo(_, gameId, dt, season, seasonType))
    writeInactivePlayres(summary.inactivePlayers, gameId, dt, season, seasonType)
    writeOfficials(summary.officials, gameId, dt, season, seasonType)
    val scoreLines = Seq(summary.homeStats.scoreLine, summary.awayStats.scoreLine).flatten
    writeScoreLines(scoreLines, dt, season, seasonType)
    val otherStats = Seq(summary.homeStats.otherStats, summary.awayStats.otherStats).flatten
    writeOtherStats(otherStats, gameId, dt, season, seasonType)
  }


  private def downloadAlBoxScoreSummaries(gameLogs: Seq[GameRecord], force: Boolean): Seq[(String, BoxScoreSummary)] = {
    gameLogs
      .map(_.gameId)
      .distinct
      .filter(v => force || GameDateMapper.gameDate(v).isEmpty)
      .map(GameIdParameter.newParameterValue)
      .flatMap(v => {
        Thread.sleep(1000)
        println(s"Downlaoding Box Score for: ${v.value}")
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

  private def writeGameSummary(summary: GameSummary, dt: String, season: Option[String], seasonType: String): Unit = {
    val rawGameSummary = RawGameSummary(summary, dt, season, seasonType)
    MySqlClient.createTable(NBATables.raw_game_summary)
    MySqlClient.insertInto(NBATables.raw_game_summary, Seq(rawGameSummary))
  }

  def readGameSummary(whereClauses: String*): Seq[RawGameSummary] = {
    MySqlClient.selectFrom[RawGameSummary](
      NBATables.raw_game_summary,
      RawGameSummary.apply,
      whereClauses:_*)
  }

  private def writeGameInfo(info: GameInfo, gameId: String, dt: String, season: Option[String], seasonType: String): Unit = {
    val rawGameInfo = RawGameInfo(info, gameId, dt, season, seasonType)
    MySqlClient.createTable(NBATables.raw_game_info)
    MySqlClient.insertInto(NBATables.raw_game_info, Seq(rawGameInfo))
  }

  def readGameInfo(whereClauses: String*): Seq[RawGameInfo] = {
    MySqlClient.selectFrom[RawGameInfo](
      NBATables.raw_game_info,
      RawGameInfo.apply,
      whereClauses:_*)
  }

  private def writeScoreLines(scoreLines: Seq[ScoreLine], dt: String, season: Option[String], seasonType: String): Unit = {
    val rawScoreLines = scoreLines.map(RawGameScoreLine(_, dt, season, seasonType))
    MySqlClient.createTable(NBATables.raw_game_score_line)
    MySqlClient.insertInto(NBATables.raw_game_score_line, rawScoreLines)
  }

  private def writeOtherStats(otherStats: Seq[OtherStats], gameId: String, dt: String, season: Option[String], seasonType: String): Unit = {
    val rawOtherStats = otherStats.map(RawOtherStats(_, gameId, dt, season, seasonType))
    MySqlClient.createTable(NBATables.raw_game_other_stats)
    MySqlClient.insertInto(NBATables.raw_game_other_stats, rawOtherStats)
  }

  private def writeOfficials(officials: Seq[Officials], gameId: String, dt: String, season: Option[String], seasonType: String): Unit = {
    val gameOfficials = officials.map(GameOfficial(_, gameId, dt, season, seasonType))
    MySqlClient.createTable(NBATables.game_officials)
    MySqlClient.insertInto(NBATables.game_officials, gameOfficials)
  }

  private def writeInactivePlayres(inactivePlayers: Seq[InactivePlayers], gameId: String, dt: String, season: Option[String], seasonType: String): Unit = {
    val rawInactivePlayers = inactivePlayers.map(v => RawInactivePlayers(v, gameId, dt, season, seasonType))
    MySqlClient.createTable(NBATables.raw_game_inactive_players)
    MySqlClient.insertInto(NBATables.raw_game_inactive_players, rawInactivePlayers)
  }
}
