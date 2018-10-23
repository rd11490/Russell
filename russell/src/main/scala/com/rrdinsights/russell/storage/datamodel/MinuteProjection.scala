package com.rrdinsights.russell.storage.datamodel

import java.sql.ResultSet

final case class MinuteProjection(Player: String,
                                  Team: String,
                                  G: Integer,
                                  MPG: Integer,
                                  Min: Integer)

object MinuteProjection extends ResultSetMapper {

  def apply(resultSet: ResultSet): MinuteProjection =
    MinuteProjection(
      Player = getString(resultSet, 0),
      Team = getString(resultSet, 1),
      G = getInt(resultSet, 2),
      MPG = getInt(resultSet, 3),
      Min = getInt(resultSet, 4)
    )
}
