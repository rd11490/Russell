package com.rrdinsights.russell.investigation

import com.rrdinsights.russell.etl.application.TeamInfo
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.tables.NBATables

object TeamMapper {

  lazy val TeamMap: Map[(Integer, String),TeamInfo] = buildTeamMap()

  private def buildTeamMap(/*IO*/): Map[(Integer, String),TeamInfo] = {
    MySqlClient.selectFrom(NBATables.team_info, TeamInfo.apply)
      .map(v => ((v.teamId, v.season), v))
      .toMap
  }

  def teamInfo(teamId: Integer, season: String): Option[TeamInfo] = {
    TeamMap.get((teamId, season))
  }
}
