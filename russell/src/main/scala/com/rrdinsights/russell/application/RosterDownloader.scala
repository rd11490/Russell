package com.rrdinsights.russell.application

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{RosterCoach, RosterPlayer}
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.CommonTeamRosterEndpoint
import com.rrdinsights.scalabrine.models.{Coach, CommonTeamRoster, Player}
import com.rrdinsights.scalabrine.parameters.{ParameterValue, SeasonParameter, TeamIdParameter}

object RosterDownloader {

  def downloadAndWriteAllRosters(season: String, dt: String): Unit = {
    val teamRoster = downloadAllRosters(season)
    writePlayerInfo(teamRoster.flatMap(_.players), season, dt)
    writeCoachInfo(teamRoster.flatMap(_.coaches), season, dt)
  }

  private def downloadTeamRoster(seasonParameter: ParameterValue, teamId: ParameterValue): CommonTeamRoster = {
    val teamRosterEndpoint = CommonTeamRosterEndpoint(teamId, seasonParameter)
    ScalabrineClient.getCommonTeamRoster(teamRosterEndpoint).commonTeamRoster
  }

  private def downloadAllRosters(season: String): Seq[CommonTeamRoster] = {
    val seasonParameter = SeasonParameter.newParameterValue(season)
    TeamIdParameter.TeamIds
      .map(v => {
        Thread.sleep(1000)
        downloadTeamRoster(seasonParameter, v)
      })
  }

  private def writePlayerInfo(players: Seq[Player], season: String, dt: String): Unit = {
    MySqlClient.createTable(NBATables.roster_player)
    val rosterPlayers = players.map(RosterPlayer(_, season, dt))
    MySqlClient.insertInto(NBATables.roster_player, rosterPlayers)
  }

  def readPlayerInfo(where: Seq[String] = Seq.empty): Seq[RosterPlayer] = {
    MySqlClient.selectFrom[RosterPlayer](
      NBATables.roster_player,
      RosterPlayer.apply,
      where:_*)
  }

  private def writeCoachInfo(coaches: Seq[Coach], season: String, dt: String): Unit = {
    MySqlClient.createTable(NBATables.roster_coach)
    val rosterCoaches = coaches.map(RosterCoach(_, season, dt))
    MySqlClient.insertInto(NBATables.roster_coach, rosterCoaches)
  }


}
