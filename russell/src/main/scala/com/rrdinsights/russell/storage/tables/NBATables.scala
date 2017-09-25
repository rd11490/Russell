package com.rrdinsights.russell.storage.tables

import com.rrdinsights.russell.storage.datamodel._

object NBATables {

  val game_record: MySqlTable = MySqlTable[GameRecord]("game_record")
  val raw_play_by_play: MySqlTable = MySqlTable[RawPlayByPlayEvent]("raw_play_by_play")
  val raw_shot_data: MySqlTable = MySqlTable[RawShotData]("raw_shot_data")
  val roster_player: MySqlTable = MySqlTable[RosterPlayer]("roster_player")
  val roster_coach: MySqlTable = MySqlTable[RosterCoach]("roster_coach")
}
