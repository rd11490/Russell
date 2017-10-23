package com.rrdinsights.russell.storage.tables

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
  val roster_player: MySqlTable = MySqlTable[RosterPlayer]("roster_player")
  val roster_coach: MySqlTable = MySqlTable[RosterCoach]("roster_coach")
  val raw_team_box_score_advanced: MySqlTable = MySqlTable[RawTeamBoxScoreAdvanced]("raw_team_box_score_advanced")
  val raw_player_box_score_advanced: MySqlTable = MySqlTable[RawPlayerBoxScoreAdvanced]("raw_player_box_score_advanced")
  val players_on_court: MySqlTable = MySqlTable[PlayersOnCourt]("players_on_court")

}
