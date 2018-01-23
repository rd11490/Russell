package com.rrdinsights.russell.etl

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.tables.{MySqlTable, NBATables}

object DatabaseExport {

  import NBATables._

  private val BasePath: String = "/Users/ryandavis/Documents/Workspace/NBAData/csv"

  private val SeasonTables: Seq[MySqlTable] = Seq(
    game_officials,
    game_record,
    raw_game_inactive_players,
    raw_game_info,
    raw_game_other_stats,
    raw_game_score_line,
    raw_game_summary,
    raw_play_by_play,
    raw_shot_data,
    raw_player_profile_season_totals,
    roster_player,
    roster_coach,
    raw_team_box_score_advanced,
    raw_player_box_score_advanced,
    players_on_court,
    players_on_court_at_period,
    lineup_shots,
    team_info,
    play_by_play_with_lineup,
    team_scored_shots,
    offense_expected_points,
    defense_expected_points,
    offense_expected_points_by_player,
    defense_expected_points_by_player,
    offense_expected_points_by_player_on_off,
    defense_expected_points_by_player_on_off,
    offense_expected_points_by_game,
    defense_expected_points_by_game,
    shot_stint_data
  )

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
    MySqlClient.selectResultSetFromAndWrite(table, fullPath, where: _*)
  }
}
