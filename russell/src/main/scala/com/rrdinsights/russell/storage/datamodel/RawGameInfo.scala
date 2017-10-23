package com.rrdinsights.russell.storage.datamodel

import java.{lang => jl}

import com.rrdinsights.scalabrine.models.GameInfo

final case class RawGameInfo(
                              primaryKey: String,
                              gameId: String,
                              gameDate: String,
                              attendance: jl.Integer,
                              gameTime: String,
                              dt: String,
                              season: String)

object RawGameInfo extends ResultSetMapper {

  def apply(gameInfo: GameInfo, gameId: String, dt: String, season: Option[String]): RawGameInfo = {
    RawGameInfo(
      gameId,
      gameId,
      gameInfo.gameDate,
      gameInfo.attendance,
      gameInfo.gameTime,
      dt,
      season.getOrElse(DataModelUtils.gameIdToSeason(gameId)))
  }
}
