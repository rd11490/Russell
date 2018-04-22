package com.rrdinsights.russell.storage.datamodel

import java.sql.ResultSet
import java.{lang => jl}

final case class ExpectedPoints(
                                 primaryKey: String,
                                 teamId: Integer,
                                 bin: String,
                                 attempts: Integer,
                                 made: Integer,
                                 value: Integer,
                                 pointsAvg: jl.Double,
                                 pointsStDev: jl.Double,
                                 expectedPointsAvg: jl.Double,
                                 expectedPointsStDev: jl.Double,
                                 season: String,
                                 dt: String)

object ExpectedPoints extends ResultSetMapper {
  def apply(resultSet: ResultSet): ExpectedPoints =
    ExpectedPoints(
      getString(resultSet, 0),
      getInt(resultSet, 1),
      getString(resultSet, 2),
      getInt(resultSet, 3),
      getInt(resultSet, 4),
      getInt(resultSet, 5),
      getDouble(resultSet, 6),
      getDouble(resultSet, 7),
      getDouble(resultSet, 8),
      getDouble(resultSet, 9),
      getString(resultSet, 10),
      getString(resultSet, 11))
}