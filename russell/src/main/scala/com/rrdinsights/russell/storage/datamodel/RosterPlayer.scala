package com.rrdinsights.russell.storage.datamodel

import java.sql.ResultSet
import java.{lang => jl}

import com.rrdinsights.scalabrine.models.Player

final case class RosterPlayer(
                               primaryKey: String,
                               teamId: jl.Integer,
                               seasonPlayed: String,
                               leagueId: String,
                               playerName: String,
                               number: String,
                               position: String,
                               height: String,
                               weight: String,
                               birthDate: String,
                               age: jl.Double,
                               experience: String,
                               school: String,
                               playerId: jl.Integer,
                               seasonParameter: String,
                               dt: String)

object RosterPlayer extends ResultSetMapper {
  def apply(player: Player, season: String, dt: String): RosterPlayer =
    RosterPlayer(
      player.playerId.toString,
      player.teamId,
      player.season,
      player.leagueId,
      player.playerName,
      player.number,
      player.position,
      player.height,
      player.weight,
      player.birthDate,
      player.age,
      player.experience,
      player.school,
      player.playerId,
      season,
      dt)

  def apply(resultSet: ResultSet): RosterPlayer =
    RosterPlayer(
      getString(resultSet, 0),
      getInt(resultSet, 1),
      getString(resultSet, 2),
      getString(resultSet, 3),
      getString(resultSet, 4),
      getString(resultSet, 5),
      getString(resultSet, 6),
      getString(resultSet, 7),
      getString(resultSet, 8),
      getString(resultSet, 9),
      getDouble(resultSet, 10),
      getString(resultSet, 11),
      getString(resultSet, 12),
      getInt(resultSet, 13),
      getString(resultSet, 14),
      getString(resultSet, 15))

}