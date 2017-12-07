package com.rrdinsights.russell.storage.tables

import com.rrdinsights.russell.etl.application.TeamInfo
import com.rrdinsights.russell.investigation.shots.PlayerShotChartSection
import com.rrdinsights.russell.investigation.shots.expectedshots.{ExpectedPoints, ExpectedPointsByGame}
import com.rrdinsights.russell.storage.datamodel._

object NBATables {

  val game_officials: MySqlTable = MySqlTable[GameOfficial]("game_officials")
  val game_record: MySqlTable = MySqlTable[GameRecord]("game_record")
  val raw_game_inactive_players: MySqlTable = MySqlTable[RawInactivePlayers]("raw_game_inactive_players")
  val raw_game_info: MySqlTable = MySqlTable[RawGameInfo]("raw_game_info")
  val raw_game_other_stats: MySqlTable = MySqlTable[RawOtherStats]("raw_game_other_stats")
  val raw_game_score_line: MySqlTable = MySqlTable[RawGameScoreLine]("raw_game_score_line")
  val raw_game_summary: MySqlTable = MySqlTable[RawGameSummary]("raw_game_summary")
  val raw_play_by_play: MySqlTable = MySqlTable[RawPlayByPlayEvent]("raw_play_by_play")
  val raw_shot_data: MySqlTable = MySqlTable[RawShotData]("raw_shot_data")
  val raw_player_profile_career_totals: MySqlTable = MySqlTable[RawPlayerProfileCareer]("raw_player_profile_career_totals")
  val raw_player_profile_season_totals: MySqlTable = MySqlTable[RawPlayerProfileCareer]("raw_player_profile_season_totals")
  val roster_player: MySqlTable = MySqlTable[RosterPlayer]("roster_player")
  val roster_coach: MySqlTable = MySqlTable[RosterCoach]("roster_coach")
  val raw_team_box_score_advanced: MySqlTable = MySqlTable[RawTeamBoxScoreAdvanced]("raw_team_box_score_advanced")
  val raw_player_box_score_advanced: MySqlTable = MySqlTable[RawPlayerBoxScoreAdvanced]("raw_player_box_score_advanced")
  val players_on_court: MySqlTable = MySqlTable[PlayersOnCourt]("players_on_court")
  val players_on_court_at_period: MySqlTable = MySqlTable[PlayersOnCourt]("players_on_court_at_period")
  val player_shot_charts: MySqlTable = MySqlTable[PlayerShotChartSection]("player_shot_charts")
  val lineup_shots: MySqlTable = MySqlTable[ShotWithPlayers]("lineup_shots")
  val team_info: MySqlTable = MySqlTable[TeamInfo]("team_info")

  val play_by_play_with_lineup: MySqlTable = MySqlTable[PlayByPlayWithLineup]("play_by_play_with_lineup")

  val team_scored_shots: MySqlTable = MySqlTable[ScoredShot]("team_scored_shots")
  val players_on_court_test: MySqlTable = MySqlTable[PlayersOnCourt]("players_on_court_test")

  val offense_expected_points_total: MySqlTable = MySqlTable[ExpectedPoints]("offense_expected_points_total")
  val offense_expected_points_zoned: MySqlTable = MySqlTable[ExpectedPoints]("offense_expected_points_zoned")
  val defense_expected_points_total: MySqlTable = MySqlTable[ExpectedPoints]("defense_expected_points_total")
  val defense_expected_points_zoned: MySqlTable = MySqlTable[ExpectedPoints]("defense_expected_points_zoned")

  val offense_expected_points_by_game_total: MySqlTable = MySqlTable[ExpectedPointsByGame]("offense_expected_points_by_game_total")
  val offense_expected_points_by_game_zoned: MySqlTable = MySqlTable[ExpectedPointsByGame]("offense_expected_points_by_game_zoned")
  val defense_expected_points_by_game_total: MySqlTable = MySqlTable[ExpectedPointsByGame]("defense_expected_points_by_game_total")
  val defense_expected_points_by_game_zoned: MySqlTable = MySqlTable[ExpectedPointsByGame]("defense_expected_points_by_game_zoned")


 }
