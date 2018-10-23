package com.rrdinsights.russell.storage.datamodel

import java.sql.ResultSet

final case class LeagueSchedule(HomeTeam: String, AwayTeam: String)

object LeagueSchedule extends ResultSetMapper {

  def apply(resultSet: ResultSet): LeagueSchedule = {
    LeagueSchedule(getString(resultSet, 0), getString(resultSet, 1))
  }
}
