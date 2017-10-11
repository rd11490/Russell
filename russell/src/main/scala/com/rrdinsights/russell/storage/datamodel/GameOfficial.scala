package com.rrdinsights.russell.storage.datamodel

import java.{lang => jl}

import com.rrdinsights.scalabrine.models.Officials

final case class GameOfficial(
                               primaryKey: String,
                               gameId: String,
                               officialId: jl.Integer,
                               firstName: String,
                               lastName: String,
                               number: String,
                               dt: String,
                               season: String)

object GameOfficial extends ResultSetMapper {

  def apply(official: Officials, gameId: String, dt: String, season: Option[String]): GameOfficial =
    GameOfficial(
      s"${gameId}_${official.officialId}",
      gameId,
      official.officialId,
      official.firstName,
      official.lastName,
      official.number,
      dt,
      season.getOrElse(DataModelUtils.gameIdToSeason(gameId)))
}