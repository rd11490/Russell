package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.RawGameSummary
import com.rrdinsights.russell.storage.tables.NBATables

object GameSummaryMap {

  lazy val GameMap: Map[String, RawGameSummary] = buildGameIdMap()

  private def buildGameIdMap( /*IO*/ ): Map[String, RawGameSummary] = {
    MySqlClient
      .selectFrom(NBATables.raw_game_summary, RawGameSummary.apply)
      .map(v => (v.gameId, v))
      .toMap
  }

}
