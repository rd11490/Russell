package com.rrdinsights.russell.storage.datamodel

import java.sql.ResultSet
import java.{lang => jl}

final case class RawPlayByPlayWithLineup(primaryKey: String,
                                          gameId: String,
                                          eventNumber: jl.Integer,
                                          playType: String,
                                          eventActionType: jl.Integer, // TODO - convert to case objects
                                          period: jl.Integer,
                                          wcTimeString: String, // TODO - find out what this means
                                          pcTimeString: String,
                                          homeDescription: String,
                                          neutralDescription: String,
                                          awayDescription: String,

                                          homeScore: jl.Integer,
                                          awayScore: jl.Integer,

                                          player1Id: jl.Integer,
                                          player2Id: jl.Integer,
                                          player3Id: jl.Integer,

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

object RawPlayByPlayWithLineup extends ResultSetMapper {
  def apply(resultSet: ResultSet): RawPlayByPlayWithLineup = {
    RawPlayByPlayWithLineup(
      getString(resultSet, 0),
      getString(resultSet, 1),
      getInt(resultSet, 2),
      getString(resultSet, 3),
      getInt(resultSet, 4),
      getInt(resultSet, 5),
      getString(resultSet, 6),
      getString(resultSet, 7),
      getString(resultSet, 8),
      getString(resultSet, 9),
      getString(resultSet, 10),

      getInt(resultSet, 11),
      getInt(resultSet, 12),
      getInt(resultSet, 13),

      getInt(resultSet, 14),
      getInt(resultSet, 15),

      getInt(resultSet, 16),
      getInt(resultSet, 17),
      getInt(resultSet, 18),
      getInt(resultSet, 19),
      getInt(resultSet, 20),
      getInt(resultSet, 21),

      getInt(resultSet, 22),
      getInt(resultSet, 23),
      getInt(resultSet, 24),
      getInt(resultSet, 25),
      getInt(resultSet, 26),
      getInt(resultSet, 27),

      getString(resultSet, 28),
      getString(resultSet, 29))
  }
}
