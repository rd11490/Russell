package com.rrdinsights.russell.etl.application

import java.sql.ResultSet

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{RawTeamBoxScoreAdvanced, ResultSetMapper}
import com.rrdinsights.russell.storage.tables.NBATables

object TeamIdMapBuilder {

  private val league: String = "League"

  def main(strings: Array[String]): Unit = {
    val teams = buildTeamMap()
    val league = addLeague(teams)
    writeTeamMap(teams ++ league)
  }


  private def buildTeamMap(/*IO*/): Seq[TeamInfo] = {
    MySqlClient.selectFrom(NBATables.raw_team_box_score_advanced, RawTeamBoxScoreAdvanced.apply)
      .map(v => TeamInfo(s"${v.teamId}_${v.season}", v.teamId, v.teamName, v.teamAbbreviation, v.teamCity, v.season))
      .distinct
  }

  private def addLeague(teamInfo: Seq[TeamInfo]): Seq[TeamInfo] =
    teamInfo
      .map(v => v.season)
      .distinct
      .map(v => TeamInfo(s"0_$v", 0, league, league, league, v))

  private def writeTeamMap(teams: Seq[TeamInfo]): Unit = {
    MySqlClient.createTable(NBATables.team_info)
    MySqlClient.insertInto(NBATables.team_info, teams)
  }

}

final case class TeamInfo(primaryKey: String,
                          teamId: Integer,
                          teamName: String,
                          teamAbbreviation: String,
                          teamCity: String,
                          season: String)

object TeamInfo extends ResultSetMapper {
  def apply(resultSet: ResultSet): TeamInfo =
    TeamInfo(
      getString(resultSet, 0),
      getInt(resultSet, 1),
      getString(resultSet, 2),
      getString(resultSet, 3),
      getString(resultSet, 4),
      getString(resultSet, 5))
}