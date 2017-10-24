package com.rrdinsights.russell.storage.datamodel

import java.sql.ResultSet
import java.{lang => jl}

final case class PlayersOnCourt(
                                 primaryKey: String,
                                 gameId: String,
                                 eventNumber: jl.Integer,
                                 teamId1: jl.Integer,
                                 team1player1Id: jl.Integer,
                                 team1player2Id: jl.Integer,
                                 team1player3Id: jl.Integer,
                                 team1player4Id: jl.Integer,
                                 team1player5Id: jl.Integer,
                                 teamId2: jl.Integer,
                                 team2player1Id: jl.Integer,
                                 team2player2Id: jl.Integer,
                                 team2player3Id: jl.Integer,
                                 team2player4Id: jl.Integer,
                                 team2player5Id: jl.Integer,
                                 dt: String,
                                 season: String)

object PlayersOnCourt extends ResultSetMapper {
  def apply(resultSet: ResultSet): PlayersOnCourt =
    PlayersOnCourt(
      getString(resultSet, 0),
      getString(resultSet, 1),
      getInt(resultSet, 2),
      getInt(resultSet, 3),
      getInt(resultSet, 4),
      getInt(resultSet, 5),
      getInt(resultSet, 6),
      getInt(resultSet, 7),
      getInt(resultSet, 8),
      getInt(resultSet, 9),
      getInt(resultSet, 10),
      getInt(resultSet, 11),
      getInt(resultSet, 12),
      getInt(resultSet, 13),
      getInt(resultSet, 14),
      getString(resultSet, 15),
      getString(resultSet, 16))
}
