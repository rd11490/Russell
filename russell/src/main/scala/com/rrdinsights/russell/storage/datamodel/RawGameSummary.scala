package com.rrdinsights.russell.storage.datamodel

import java.sql.ResultSet
import java.{lang => jl}

import com.rrdinsights.scalabrine.models.GameSummary

final case class RawGameSummary(
                                 primaryKey: String,
                                 gameDate: String,
                                 gameSequence: jl.Integer,
                                 gameId: String,
                                 gameStatusId: jl.Integer,
                                 gameStatusText: String,
                                 gameCode: String,
                                 homeTeamId: jl.Integer,
                                 awayTeamId: jl.Integer,
                                 seasonYear: String,
                                 livePeriod: jl.Integer,
                                 livePCTime: String,
                                 broadcaster: String,
                                 livePeriodTimeBroadcast: String,
                                 whStatus: jl.Integer,
                                 dt: String,
                                 season: String)

object RawGameSummary extends ResultSetMapper {
  def apply(gameSummary: GameSummary, dt: String, season: Option[String]): RawGameSummary =
    RawGameSummary(
      gameSummary.gameId,
      gameSummary.gameDate,
      gameSummary.gameSequence,
      gameSummary.gameId,
      gameSummary.gameStatusId,
      gameSummary.gameStatusText,
      gameSummary.gameCode,
      gameSummary.homeTeamId,
      gameSummary.awayTeamId,
      gameSummary.season,
      gameSummary.livePeriod,
      gameSummary.livePCTime,
      gameSummary.broadcaster,
      gameSummary.livePeriodTimeBroadcast,
      gameSummary.whStatus,
      dt,
      season.getOrElse(DataModelUtils.gameIdToSeason(gameSummary.gameId)))

  def apply(resultSet: ResultSet): RawGameSummary =
    RawGameSummary(
      getString(resultSet, 0),
      getString(resultSet, 1),
      getInt(resultSet, 2),
      getString(resultSet, 3),
      getInt(resultSet, 4),
      getString(resultSet, 5),
      getString(resultSet, 6),
      getInt(resultSet, 7),
      getInt(resultSet, 8),
      getString(resultSet, 9),
      getInt(resultSet, 10),
      getString(resultSet, 11),
      getString(resultSet, 12),
      getString(resultSet, 13),
      getInt(resultSet, 14),
      getString(resultSet, 15),
      getString(resultSet, 16))
}