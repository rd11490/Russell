package com.rrdinsights.russell.storage.datamodel

import java.sql.ResultSet
import java.{lang => jl}

import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.Serialization.write

final case class ShotWithPlayers(primaryKey: String,
                                 gameId: String,
                                 eventNumber: jl.Integer,
                                 shooter: jl.Integer,
                                 offenseTeamId: jl.Integer,
                                 offensePlayer1Id: jl.Integer,
                                 offensePlayer2Id: jl.Integer,
                                 offensePlayer3Id: jl.Integer,
                                 offensePlayer4Id: jl.Integer,
                                 offensePlayer5Id: jl.Integer,
                                 defenseTeamId: jl.Integer,
                                 defensePlayer1Id: jl.Integer,
                                 defensePlayer2Id: jl.Integer,
                                 defensePlayer3Id: jl.Integer,
                                 defensePlayer4Id: jl.Integer,
                                 defensePlayer5Id: jl.Integer,
                                 shotDistance: jl.Integer,
                                 xCoordinate: jl.Integer,
                                 yCoordinate: jl.Integer,
                                 shotZone: String,
                                 shotAttemptedFlag: jl.Integer,
                                 shotMadeFlag: jl.Integer,
                                 shotValue: jl.Integer,
                                 period: jl.Integer,
                                 minutesRemaining: jl.Integer,
                                 secondsRemaining: jl.Integer,
                                 gameDate: jl.Long,
                                 season: String,
                                 seasonType: String,
                                 dt: String)

object ShotWithPlayers extends ResultSetMapper {

  private implicit val formats: Formats = DefaultFormats

  def shotsToJson(shots: Seq[ShotWithPlayers]): String = write(shots)

  def apply(resultSet: ResultSet): ShotWithPlayers =
    ShotWithPlayers(
      primaryKey = getString(resultSet, 0),
      gameId = getString(resultSet, 1),
      eventNumber = getInt(resultSet, 2),
      shooter = getInt(resultSet, 3),
      offenseTeamId = getInt(resultSet, 4),
      offensePlayer1Id = getInt(resultSet, 5),
      offensePlayer2Id = getInt(resultSet, 6),
      offensePlayer3Id = getInt(resultSet, 7),
      offensePlayer4Id = getInt(resultSet, 8),
      offensePlayer5Id = getInt(resultSet, 9),
      defenseTeamId = getInt(resultSet, 10),
      defensePlayer1Id = getInt(resultSet, 11),
      defensePlayer2Id = getInt(resultSet, 12),
      defensePlayer3Id = getInt(resultSet, 13),
      defensePlayer4Id = getInt(resultSet, 14),
      defensePlayer5Id = getInt(resultSet, 15),
      shotDistance = getInt(resultSet, 16),
      xCoordinate = getInt(resultSet, 17),
      yCoordinate = getInt(resultSet, 18),
      shotZone = getString(resultSet, 19),
      shotAttemptedFlag = getInt(resultSet, 20),
      shotMadeFlag = getInt(resultSet, 21),
      shotValue = getInt(resultSet, 22),
      period = getInt(resultSet, 23),
      minutesRemaining = getInt(resultSet, 24),
      secondsRemaining = getInt(resultSet, 25),
      gameDate = getLong(resultSet, 26),
      season = getString(resultSet, 27),
      seasonType = getString(resultSet, 28),
      dt = getString(resultSet, 29)
    )
}
