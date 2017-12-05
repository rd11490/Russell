package com.rrdinsights.russell.storage.datamodel

import java.sql.ResultSet
import java.{lang => jl}

import com.rrdinsights.scalabrine.models.PlayerProfileCareer

final case class RawPlayerProfileCareer(
                                         primaryKey: String,
                                         playerId: Integer,
                                         gamesPlayed: Integer,
                                         gamesStarted: Integer,
                                         minutes: jl.Double,
                                         fieldGoalsMade: jl.Double,
                                         fieldGoalsAttempted: jl.Double,
                                         fieldGoalPercent: jl.Double,
                                         threePointFieldGoalsMade: jl.Double,
                                         threePointFieldGoalsAttempted: jl.Double,
                                         threePointFieldGoalPercent: jl.Double,
                                         freeThrowsMade: jl.Double,
                                         freeThrowsAttempted: jl.Double,
                                         freeThrowPercent: jl.Double,
                                         offensiveRebounds: jl.Double,
                                         defensiveRebounds: jl.Double,
                                         rebounds: jl.Double,
                                         assists: jl.Double,
                                         steals: jl.Double,
                                         blocks: jl.Double,
                                         turnovers: jl.Double,
                                         fouls: jl.Double,
                                         points: jl.Double,
                                         dt: String)


object RawPlayerProfileCareer extends ResultSetMapper {

  def apply(profile: PlayerProfileCareer, dt: String): RawPlayerProfileCareer = {
    RawPlayerProfileCareer(
      s"${profile.playerId}",
      profile.playerId,
      profile.gamesPlayed,
      profile.gamesStarted,
      profile.minutes,
      profile.fieldGoalsMade,
      profile.fieldGoalsAttempted,
      profile.fieldGoalPercent,
      profile.threePointFieldGoalsMade,
      profile.threePointFieldGoalsAttempted,
      profile.threePointFieldGoalPercent,
      profile.freeThrowsMade,
      profile.freeThrowsAttempted,
      profile.freeThrowPercent,
      profile.offensiveRebounds,
      profile.defensiveRebounds,
      profile.rebounds,
      profile.assists,
      profile.steals,
      profile.blocks,
      profile.turnovers,
      profile.fouls,
      profile.points,
      dt)
  }

  def apply(resultSet: ResultSet): RawPlayerProfileCareer =
    RawPlayerProfileCareer(
      getString(resultSet, 0),
      getInt(resultSet, 1),
      getInt(resultSet, 2),
      getInt(resultSet, 3),
      getDouble(resultSet, 4),
      getDouble(resultSet, 5),
      getDouble(resultSet, 6),
      getDouble(resultSet, 7),
      getDouble(resultSet, 8),
      getDouble(resultSet, 9),
      getDouble(resultSet, 10),
      getDouble(resultSet, 11),
      getDouble(resultSet, 12),
      getDouble(resultSet, 13),
      getDouble(resultSet, 14),
      getDouble(resultSet, 15),
      getDouble(resultSet, 16),
      getDouble(resultSet, 17),
      getDouble(resultSet, 18),
      getDouble(resultSet, 19),
      getDouble(resultSet, 20),
      getDouble(resultSet, 21),
      getDouble(resultSet, 22),
      getString(resultSet, 23))
}