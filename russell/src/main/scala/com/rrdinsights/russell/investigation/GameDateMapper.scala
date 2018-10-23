package com.rrdinsights.russell.investigation

import com.rrdinsights.russell.etl.application.GameDate
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.tables.NBATables

object GameDateMapper {

  lazy val GameMap: Map[String, GameDate] = buildGameIdMap()

  private def buildGameIdMap( /*IO*/ ): Map[String, GameDate] = {
    MySqlClient
      .selectFrom(NBATables.game_dates, GameDate.apply)
      .map(v => (v.gameId, v))
      .toMap
  }

  def gameDate(gameId: String): Option[GameDate] = {
    GameMap.get(gameId)
  }
}
