package com.rrdinsights.russell.investigation

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.RawPlayerProfileSeason
import com.rrdinsights.russell.storage.tables.NBATables

object PlayerTeamMapper {

  lazy val PlayerTeamMap: Map[(Integer, String), (Integer, String)] =
    buildPlayerTeamMap()

  private def buildPlayerTeamMap(
      /*IO*/ ): Map[(Integer, String), (Integer, String)] = {
    MySqlClient
      .selectFrom(NBATables.raw_player_profile_season_totals,
                  RawPlayerProfileSeason.apply)
      .flatMap(
        v =>
          TeamMapper
            .latestTeamInfo(v.teamId)
            .map(c => ((v.playerId, v.season), (v.teamId, c.teamAbbreviation))))
      .distinct
      .toMap
  }

  def lookupTeam(id: Integer, season: String): (Integer, String) = {
    PlayerTeamMap.getOrElse((id, season), (-1, "N/A"))
  }
}
