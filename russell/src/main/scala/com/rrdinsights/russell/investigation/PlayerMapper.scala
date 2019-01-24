package com.rrdinsights.russell.investigation

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.RawPlayerBoxScoreAdvanced
import com.rrdinsights.russell.storage.tables.NBATables

object PlayerMapper {

  lazy val PlayerMap: Map[Integer, String] = buildPlayerMap()

  lazy val PlayerTeamMap: Map[(Integer, String), (Integer, String)] = buildPlayerTeamMap()


  private def buildPlayerMap(/*IO*/): Map[Integer, String] = {
    MySqlClient.selectFrom(NBATables.raw_player_box_score_advanced, RawPlayerBoxScoreAdvanced.apply)
      .map(v => (v.playerId, v.playerName))
      .distinct
      .toMap
  }

  private def buildPlayerTeamMap(/*IO*/): Map[(Integer, String), (Integer, String)] = {
    MySqlClient.selectFrom(NBATables.raw_player_box_score_advanced, RawPlayerBoxScoreAdvanced.apply)
      .map(v => ((v.playerId, v.season), (v.teamId, v.teamAbbreviation)))
      .toMap
  }

  def lookup(id: Integer): String  = PlayerMap.getOrElse(id, "N/A")

  def lookupTeam(id: Integer, season: String): (Integer, String) = {
    PlayerTeamMap.getOrElse((id, season), (-1, "N/A"))
  }
}
