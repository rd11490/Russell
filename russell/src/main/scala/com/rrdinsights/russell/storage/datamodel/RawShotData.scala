package com.rrdinsights.russell.storage.datamodel

import java.sql.ResultSet
import java.{lang => jl}

import com.rrdinsights.scalabrine.models.Shot

final case class RawShotData(
                              primaryKey: String,
                              gridType: String,
                              gameId: String,
                              eventNumber: jl.Integer,
                              playerId: jl.Integer,
                              playerName: String,
                              teamId: jl.Integer,
                              teamName: String,
                              period: jl.Integer,
                              minutesRemaining: jl.Integer,
                              secondsRemaining: jl.Integer,
                              eventType: String,
                              actionType: String,
                              shotZoneBasic: String,
                              shotZoneArea: String,
                              shotZoneRange: String,
                              shotType: String,
                              shotDistance: jl.Integer,
                              xCoordinate: jl.Integer,
                              yCoordinate: jl.Integer,
                              shotAttemptedFlag: jl.Integer,
                              shotMadeFlag: jl.Integer,
                              gameDate: String,
                              homeTeam: String,
                              awayTeam: String,
                              season: String,
                              dt: String)

object RawShotData extends ResultSetMapper {
  def apply(shot: Shot, season: String, dt: String): RawShotData = {
    val seasonOfShot = if (season != "") season else DataModelUtils.gameIdToSeason(shot.gameId)
    RawShotData(
      s"${shot.gameId}_${shot.gameEventId}_${shot.playerId}",
      shot.gridType,
      shot.gameId,
      shot.gameEventId,
      shot.playerId,
      shot.playerName,
      shot.teamId,
      shot.teamName,
      shot.period,
      shot.minutesRemaining,
      shot.secondsRemaining,
      shot.eventType,
      shot.actionType,
      shot.shotZoneBasic,
      shot.shotZoneArea,
      shot.shotZoneRange,
      shot.shotType,
      shot.shotDistance,
      shot.xCoordinate,
      shot.yCoordinate,
      shot.shotAttemptedFlag,
      shot.shotMadeFlag,
      shot.gameDate,
      shot.homeTeam,
      shot.awayTeam,
      seasonOfShot,
      dt)
  }

  def apply(resultSet: ResultSet): RawShotData =
    RawShotData(
      getString(resultSet, 0),
      getString(resultSet, 1),
      getString(resultSet, 2),
      getInt(resultSet, 3),
      getInt(resultSet, 4),
      getString(resultSet, 5),
      getInt(resultSet, 6),
      getString(resultSet, 7),
      getInt(resultSet, 8),
      getInt(resultSet, 9),
      getInt(resultSet, 10),
      getString(resultSet, 11),
      getString(resultSet, 12),
      getString(resultSet, 13),
      getString(resultSet, 14),
      getString(resultSet, 15),
      getString(resultSet, 16),
      getInt(resultSet, 17),
      getInt(resultSet, 18),
      getInt(resultSet, 19),
      getInt(resultSet, 20),
      getInt(resultSet, 21),
      getString(resultSet, 22),
      getString(resultSet, 23),
      getString(resultSet, 24),
      getString(resultSet, 25),
      getString(resultSet, 26))
}