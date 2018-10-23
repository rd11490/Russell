package com.rrdinsights.russell.storage.tables

import com.rrdinsights.russell.etl.application.{GameDate, PlayerInfo, TeamInfo}
import com.rrdinsights.russell.investigation.movement.{EventRow, PlayerWithBall}
import com.rrdinsights.russell.investigation.playbyplay.UnitPlayerStats
import com.rrdinsights.russell.investigation.playbyplay.luckadjusted.{LuckAdjustedOneWayStint, LuckAdjustedStint, LuckAdjustedUnit, SecondsPlayedContainer}
import com.rrdinsights.russell.investigation.shots.expectedshots.{ExpectedPointsByGame, ExpectedPointsPlayer, ExpectedPointsPlayerOnOff}
import com.rrdinsights.russell.investigation.shots.shotmover.ShotsSeen
import com.rrdinsights.russell.investigation.shots.{PlayerShotChartSection, ShotStintByZoneData}
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
  val raw_player_profile_season_totals: MySqlTable = MySqlTable[RawPlayerProfileSeason]("raw_player_profile_season_totals")
  val roster_player: MySqlTable = MySqlTable[RosterPlayer]("roster_player")
  val roster_coach: MySqlTable = MySqlTable[RosterCoach]("roster_coach")
  val raw_team_box_score_advanced: MySqlTable = MySqlTable[RawTeamBoxScoreAdvanced]("raw_team_box_score_advanced")
  val raw_player_box_score_advanced: MySqlTable = MySqlTable[RawPlayerBoxScoreAdvanced]("raw_player_box_score_advanced")
  val players_on_court: MySqlTable = MySqlTable[PlayersOnCourt]("players_on_court")
  val players_on_court_at_period: MySqlTable = MySqlTable[PlayersOnCourt]("players_on_court_at_period")
  val player_shot_charts: MySqlTable = MySqlTable[PlayerShotChartSection]("player_shot_charts")
  val lineup_shots: MySqlTable = MySqlTable[ShotWithPlayers]("lineup_shots")
  val team_info: MySqlTable = MySqlTable[TeamInfo]("team_info")
  val game_dates: MySqlTable = MySqlTable[GameDate]("game_date")
  val shots_seen: MySqlTable = MySqlTable[ShotsSeen]("shots_seen")
  val player_info: MySqlTable = MySqlTable[PlayerInfo]("player_info")

  val play_by_play_with_lineup: MySqlTable = MySqlTable[PlayByPlayWithLineup]("play_by_play_with_lineup")

  val team_scored_shots: MySqlTable = MySqlTable[ScoredShot]("team_scored_shots")
  val players_on_court_test: MySqlTable = MySqlTable[PlayersOnCourt]("players_on_court_test")

  val offense_expected_points: MySqlTable = MySqlTable[ExpectedPoints]("offense_expected_points")
  val defense_expected_points: MySqlTable = MySqlTable[ExpectedPoints]("defense_expected_points")

  val offense_expected_points_by_player: MySqlTable = MySqlTable[ExpectedPointsPlayer]("offense_expected_points_by_player")
  val defense_expected_points_by_player: MySqlTable = MySqlTable[ExpectedPointsPlayer]("defense_expected_points_by_player")

  val offense_expected_points_by_player_on_off: MySqlTable = MySqlTable[ExpectedPointsPlayerOnOff]("offense_expected_points_by_player_on_off_zoned")
  val defense_expected_points_by_player_on_off: MySqlTable = MySqlTable[ExpectedPointsPlayerOnOff]("defense_expected_points_by_player_on_off_zoned")


  val offense_expected_points_by_game: MySqlTable = MySqlTable[ExpectedPointsByGame]("offense_expected_points_by_game")
  val defense_expected_points_by_game: MySqlTable = MySqlTable[ExpectedPointsByGame]("defense_expected_points_by_game")

  val shot_stint_data: MySqlTable = MySqlTable[ShotStintByZoneData]("shot_stint_data")

  val movement_data: MySqlTable = MySqlTable[EventRow]("movement_data")
  val player_with_ball: MySqlTable = MySqlTable[PlayerWithBall]("player_with_ball")

  val luck_adjusted_stints: MySqlTable = MySqlTable[LuckAdjustedStint]("luck_adjusted_stints")
  val luck_adjusted_one_way_stints: MySqlTable = MySqlTable[LuckAdjustedOneWayStint]("luck_adjusted_one_way_stints")
  val seconds_played: MySqlTable = MySqlTable[SecondsPlayedContainer]("seconds_played")

  val luck_adjusted_units: MySqlTable = MySqlTable[LuckAdjustedUnit]("luck_adjusted_units")

  val player_stats_by_unit: MySqlTable = MySqlTable[UnitPlayerStats]("player_stats_by_unit")

  val real_adjusted_four_factors: MySqlTable = MySqlTable[RealAdjustedFourFactors]("real_adjusted_four_factors")
  val real_adjusted_four_factors_multi: MySqlTable = MySqlTable[RealAdjustedFourFactors]("real_adjusted_four_factors_multi")


  val minute_projection_2018_19: MySqlTable = MySqlTable[MinuteProjection]("2018_19_minute_projections")

  val league_results: MySqlTable = MySqlTable[LeagueSchedule]("league_results")


}
