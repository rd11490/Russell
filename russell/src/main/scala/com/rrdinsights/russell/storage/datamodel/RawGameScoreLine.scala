package com.rrdinsights.russell.storage.datamodel

import java.{lang => jl}

import com.rrdinsights.scalabrine.models.ScoreLine

final case class RawGameScoreLine(
                                   primaryKey: String,
                                   gameDateTimeEST: String,
                                   gameSequence: jl.Integer,
                                   gameId: String,
                                   teamId: jl.Integer,
                                   teamAbbreviation: String,
                                   teamCityName: String,
                                   teamNickName: String,
                                   teamWinsLosses: String,
                                   quarter1Points: jl.Integer,
                                   quarter2Points: jl.Integer,
                                   quarter3Points: jl.Integer,
                                   quarter4Points: jl.Integer,
                                   ot1Points: jl.Integer,
                                   ot2Points: jl.Integer,
                                   ot3Points: jl.Integer,
                                   ot4Points: jl.Integer,
                                   ot5Points: jl.Integer,
                                   ot6Points: jl.Integer,
                                   ot7Points: jl.Integer,
                                   ot8Points: jl.Integer,
                                   ot9Points: jl.Integer,
                                   ot10Points: jl.Integer,
                                   points: jl.Integer,
                                   dt: String,
                                   season: String,
                                   seasonType: String)

object RawGameScoreLine {

  def apply(scoreLine: ScoreLine, dt: String, season: Option[String], seasonType: String): RawGameScoreLine =
  RawGameScoreLine(
    s"${scoreLine.gameId}_${scoreLine.teamId}",
    scoreLine.gameDateTimeEST,
    scoreLine.gameSequence,
    scoreLine.gameId,
    scoreLine.teamId,
    scoreLine.teamAbbreviation,
    scoreLine.teamCityName,
    scoreLine.teamNickName,
    scoreLine.teamWinsLosses,
    scoreLine.quarter1Points,
    scoreLine.quarter2Points,
    scoreLine.quarter3Points,
    scoreLine.quarter4Points,
    scoreLine.ot1Points,
    scoreLine.ot2Points,
    scoreLine.ot3Points,
    scoreLine.ot4Points,
    scoreLine.ot5Points,
    scoreLine.ot6Points,
    scoreLine.ot7Points,
    scoreLine.ot8Points,
    scoreLine.ot9Points,
    scoreLine.ot10Points,
    scoreLine.points,
    dt,
    season.getOrElse(DataModelUtils.gameIdToSeason(scoreLine.gameId)),
    seasonType)
}