package com.rrdinsights.russell.storage.datamodel

import java.sql.ResultSet
import java.{lang => jl}

import com.rrdinsights.scalabrine.models.TeamStats

final case class RawTeamBoxScoreAdvanced(
                                          primaryKey: String,
                                          gameId: String,
                                          teamId: jl.Integer,
                                          teamName: String,
                                          teamAbbreviation: String,
                                          teamCity: String,
                                          minutes: jl.Double,
                                          offensiveRating: jl.Double,
                                          defensiveRating: jl.Double,
                                          netRating: jl.Double,
                                          assistPercentage: jl.Double,
                                          assistTurnOverRatio: jl.Double,
                                          assistRatio: jl.Double,
                                          offensiveReboundPercentage: jl.Double,
                                          defensiveReboundPercentage: jl.Double,
                                          reboundPercentage: jl.Double,
                                          teamTurnOverPercentage: jl.Double,
                                          effectiveFieldGoalPercentage: jl.Double,
                                          trueShootingPercentage: jl.Double,
                                          usageRate: jl.Double,
                                          pace: jl.Double,
                                          playerEstimatedImpact: jl.Double,
                                          season: String,
                                          seasonType: String,
                                          dt: String) {

}

object RawTeamBoxScoreAdvanced extends ResultSetMapper {

  def apply(teamStats: TeamStats, season: String, dt: String, seasonType: String): RawTeamBoxScoreAdvanced = {
    val seasonStr = if (season != "") season else DataModelUtils.gameIdToSeason(teamStats.gameId)
    RawTeamBoxScoreAdvanced(
      s"${teamStats.gameId}_${teamStats.teamId}",
      teamStats.gameId,
      teamStats.teamId,
      teamStats.teamName,
      teamStats.teamAbbreviation,
      teamStats.teamCity,
      teamStats.minutes,
      teamStats.offensiveRating,
      teamStats.defensiveRating,
      teamStats.netRating,
      teamStats.assistPercentage,
      teamStats. assistTurnOverRatio,
      teamStats.assistRatio,
      teamStats.offensiveReboundPercentage,
      teamStats.defensiveReboundPercentage,
      teamStats.reboundPercentage,
      teamStats.teamTurnOverPercentage,
      teamStats.effectiveFieldGoalPercentage,
      teamStats.trueShootingPercentage,
      teamStats.usage,
      teamStats.pace,
      teamStats.playerEstimatedImpact,
      seasonStr,
      seasonType,
      dt)
  }

  def apply(resultSet: ResultSet): RawTeamBoxScoreAdvanced =
    RawTeamBoxScoreAdvanced(
      getString(resultSet, 0),
      getString(resultSet, 1),
      getInt(resultSet, 2),
      getString(resultSet, 3),
      getString(resultSet, 4),
      getString(resultSet, 5),
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
      getString(resultSet, 22),
      getString(resultSet, 23),
      getString(resultSet, 24))
}