package com.rrdinsights.russell.storage.datamodel

import java.sql.ResultSet
import java.{lang => jl}

import com.rrdinsights.scalabrine.models.GameLog

final case class GameRecord(
                             primaryKey: String,
                             teamId: jl.Integer,
                             gameId: String,
                             gameDate: String,
                             matchup: String,
                             result: String,
                             wins: jl.Integer,
                             losses: jl.Integer,
                             winPercentage: jl.Double,
                             minutes: jl.Double,

                             fieldGoalsMade: jl.Integer,
                             fieldGoalAttempts: jl.Integer,
                             fieldGoalPercentage: jl.Double,

                             threePointFieldGoalsMade: jl.Integer,
                             threePointFieldGoalAttempts: jl.Integer,
                             threePointFieldGoalPercentage: jl.Double,

                             freeThrowsMade: jl.Integer,
                             freeThrowAttempts: jl.Integer,
                             freeThrowPercentage: jl.Double,

                             offensiveRebounds: jl.Integer,
                             defensiveRebounds: jl.Integer,
                             totalRebounds: jl.Integer,

                             assists: jl.Integer,
                             steals: jl.Integer,
                             blocks: jl.Integer,
                             turnovers: jl.Integer,
                             personalFouls: jl.Integer,
                             points: jl.Integer,
                             season: String,
                             seasonType: String,
                             dt: String)

object GameRecord extends ResultSetMapper {
  def apply(gameLog: GameLog, season: String, dt: String, seasonType: String): GameRecord =
    GameRecord(
      s"${gameLog.teamId}_${gameLog.gameId}",
      gameLog.teamId,
      gameLog.gameId,
      gameLog.gameDate,
      gameLog.matchup,
      gameLog.result,
      gameLog.wins,
      gameLog.losses,
      gameLog.winPercentage,
      gameLog.minutes,

      gameLog.fieldGoalsMade,
      gameLog.fieldGoalAttempts,
      gameLog.fieldGoalPercentage,

      gameLog.threePointFieldGoalsMade,
      gameLog.threePointFieldGoalAttempts,
      gameLog.threePointFieldGoalPercentage,

      gameLog.freeThrowsMade,
      gameLog.freeThrowAttempts,
      gameLog.freeThrowPercentage,

      gameLog.offensiveRebounds,
      gameLog.defensiveRebounds,
      gameLog.totalRebounds,

      gameLog.assists,
      gameLog.steals,
      gameLog.blocks,
      gameLog.turnovers,
      gameLog.personalFouls,
      gameLog.points,
      season,
      seasonType,
      dt)

  def apply(resultSet: ResultSet): GameRecord = {
    GameRecord(
      getString(resultSet, 0),
      getInt(resultSet, 1),
      getString(resultSet, 2),
      getString(resultSet, 3),
      getString(resultSet, 4),
      getString(resultSet, 5),
      getInt(resultSet, 6),
      getInt(resultSet, 7),
      getDouble(resultSet, 8),
      getDouble(resultSet, 9),
      getInt(resultSet, 10),
      getInt(resultSet, 11),
      getDouble(resultSet, 12),
      getInt(resultSet, 13),
      getInt(resultSet, 14),
      getDouble(resultSet, 15),
      getInt(resultSet, 16),
      getInt(resultSet, 17),
      getDouble(resultSet, 18),
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
      getString(resultSet, 29),
      getString(resultSet, 30))
  }
}

