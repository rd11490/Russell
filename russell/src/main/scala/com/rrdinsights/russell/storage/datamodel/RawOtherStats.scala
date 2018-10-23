package com.rrdinsights.russell.storage.datamodel

import java.{lang => jl}

import com.rrdinsights.scalabrine.models.OtherStats

final case class RawOtherStats(
                                primaryKey: String,
                                gameId: String,
                                leagueId: String,
                                teamId: jl.Integer,
                                teamAbbreviation: String,
                                teamCity: String,
                                pointsInPaint: jl.Integer,
                                secondChancePoints: jl.Integer,
                                fastBreakPoints: jl.Integer,
                                largestLead: jl.Integer,
                                leadChanges: jl.Integer,
                                timesTied: jl.Integer,
                                teamTurnovers: jl.Integer,
                                totalTurnovers: jl.Integer,
                                teamRebounds: jl.Integer,
                                pointsOffTurnOvers: jl.Integer,
                                dt: String,
                                season: String,
                                seasonType: String)

object RawOtherStats {

  def apply(otherStats: OtherStats, gameId: String, dt: String, season: Option[String], seasonType: String): RawOtherStats =
    RawOtherStats(
      s"${gameId}_${otherStats.teamId}",
      gameId,
      otherStats.leagueId,
      otherStats.teamId,
      otherStats.teamAbbreviation,
      otherStats.teamCity,
      otherStats.pointsInPaint,
      otherStats.secondChancePoints,
      otherStats.fastBreakPoints,
      otherStats.largestLead,
      otherStats.leadChanges,
      otherStats.timesTied,
      otherStats.teamTurnovers,
      otherStats.totalTurnovers,
      otherStats.teamRebounds,
      otherStats.pointsOffTurnOvers,
      dt,
      season.getOrElse(DataModelUtils.gameIdToSeason(gameId)),
      seasonType)
}