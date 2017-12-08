package com.rrdinsights.russell.etl

import java.io.File

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel._
import com.rrdinsights.russell.storage.tables.{MySqlTable, NBATables}

object DatabaseExport {

  import NBATables._

  private val BasePath: String = "/Users/ryandavis/Documents/Workspace/NBAData/csv"

  private val SeasonTables: Seq[MySqlTable] = Seq(
    game_officials,
    game_record, //[GameRecord]("game_record")
    raw_game_inactive_players, //[RawInactivePlayers]("raw_game_inactive_players")
    raw_game_info, //[RawGameInfo]("raw_game_info")
    raw_game_other_stats, //[RawOtherStats]("raw_game_other_stats")
    raw_game_score_line, //[RawGameScoreLine]("raw_game_score_line")
    raw_game_summary, //[RawGameSummary]("raw_game_summary")
    raw_play_by_play, //[RawPlayByPlayEvent]("raw_play_by_play")
    raw_shot_data, //[RawShotData]("raw_shot_data")

    //raw_player_profile_season_totals, //[RawPlayerProfileCareer]("raw_player_profile_season_totals")
    roster_player, //[RosterPlayer]("roster_player")
    roster_coach, //[RosterCoach]("roster_coach")
    raw_team_box_score_advanced, //[RawTeamBoxScoreAdvanced]("raw_team_box_score_advanced")
    raw_player_box_score_advanced, //[RawPlayerBoxScoreAdvanced]("raw_player_box_score_advanced")
    players_on_court, //[PlayersOnCourt]("players_on_court")
    players_on_court_at_period, //[PlayersOnCourt]("players_on_court_at_period")
    lineup_shots, //[ShotWithPlayers]("lineup_shots")
    team_info, //[TeamInfo]("team_info")
    play_by_play_with_lineup, //[PlayByPlayWithLineup]("play_by_play_with_lineup")
    team_scored_shots) //ScoredShot.apply)

  private val SeasonlessTables: Seq[MySqlTable] = Seq(
    player_shot_charts,
    raw_player_profile_career_totals)

  def main(strings: Array[String]): Unit = {
    SeasonTables.foreach(exportSeasonTable)
    SeasonlessTables.foreach(exportSeasonTableSeasonData(_))
  }

  private def exportSeasonTable(table: MySqlTable): Unit =
    MySqlClient.selectSeasonsFrom(table).distinct.foreach(v => exportSeasonTableSeasonData(table, Some(v)))

  private def exportSeasonTableSeasonData(table: MySqlTable, season: Option[String] = None): Unit = {
    println(season)
    val where = season.map(v => Seq(s"season = '$v'")).getOrElse(Seq.empty)
    val folderName = table.name
    val fileName = season.map(v => s"${table.name}_$v").getOrElse(s"${table.name}")
    val fullPath = s"$BasePath/$folderName/$fileName.csv"
    MySqlClient.selectResultSetFromAndWrite(table, fullPath, where:_*)
  }
}
