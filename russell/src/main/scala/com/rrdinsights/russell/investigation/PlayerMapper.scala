package com.rrdinsights.russell.investigation

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.RawPlayerBoxScoreAdvanced
import com.rrdinsights.russell.storage.tables.NBATables

object PlayerMapper {

  lazy val PlayerMap: Map[Integer, String] = buildPlayerMap()

  private def buildPlayerMap(/*IO*/): Map[Integer, String] = {
    MySqlClient.selectFrom(NBATables.raw_player_box_score_advanced, RawPlayerBoxScoreAdvanced.apply)
      .map(v => (v.playerId, v.playerName))
      .distinct
      .toMap
  }

  def lookup(id: Integer): String  = PlayerMap.getOrElse(id, "N/A")
}
