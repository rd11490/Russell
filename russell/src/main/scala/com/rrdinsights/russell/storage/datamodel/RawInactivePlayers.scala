package com.rrdinsights.russell.storage.datamodel

import java.{lang => jl}

import com.rrdinsights.scalabrine.models.InactivePlayers

final case class RawInactivePlayers(
                                     primaryKey: String,
                                     gameId: String,
                                     playerId: jl.Integer,
                                     firstName: String,
                                     lastName: String,
                                     number: String,
                                     teamId: jl.Integer,
                                     teamCity: String,
                                     teamName: String,
                                     teamAbbreviation: String,
                                     dt: String,
                                     season: String)

object RawInactivePlayers extends ResultSetMapper {
  def apply(inactivePlayers: InactivePlayers, gameId: String, dt: String, season: Option[String]): RawInactivePlayers =
    RawInactivePlayers(
      s"${gameId}_${inactivePlayers.playerId}",
      gameId,
      inactivePlayers.playerId,
      inactivePlayers.firstName,
      inactivePlayers.lastName,
      inactivePlayers.number,
      inactivePlayers.teamId,
      inactivePlayers.teamCity,
      inactivePlayers.teamName,
      inactivePlayers.teamAbbreviation,
      dt,
      season.getOrElse(DataModelUtils.gameIdToSeason(gameId)))
}