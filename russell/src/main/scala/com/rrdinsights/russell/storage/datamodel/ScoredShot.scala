package com.rrdinsights.russell.storage.datamodel

import java.sql.ResultSet
import java.{lang => jl}

final case class ScoredShot(primaryKey: String,
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
                            bin: String,
                            shotValue: jl.Integer,
                            shotAttempted: jl.Integer,
                            shotMade: jl.Integer,
                            expectedPoints: jl.Double,
                            playerShotAttempted: jl.Integer,
                            playerShotMade: jl.Integer,
                            season: String,
                            seasonType: String,
                            dt: String)

object ScoredShot extends ResultSetMapper {
  def apply(resultSet: ResultSet): ScoredShot =
    ScoredShot(
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
      getInt(resultSet, 15),

      getString(resultSet, 16),

      getInt(resultSet, 17),
      getInt(resultSet, 18),
      getInt(resultSet, 19),
      getDouble(resultSet, 20),
      getInt(resultSet, 21),
      getInt(resultSet, 22),

      getString(resultSet, 23),
      getString(resultSet, 24),
      getString(resultSet, 25))
}