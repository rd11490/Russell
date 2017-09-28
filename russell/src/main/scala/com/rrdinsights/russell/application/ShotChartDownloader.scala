package com.rrdinsights.russell.application

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.RawShotData
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.ShotChartDetailEndpoint
import com.rrdinsights.scalabrine.models.Shot
import com.rrdinsights.scalabrine.parameters.{ParameterValue, PlayerIdParameter, SeasonParameter}

object ShotChartDownloader {

  def downloadAndWritePlayersShotData(playerIds: Seq[String], dt: String, season: String = ""): Unit = {
    val shotData = downloadPlayersShotData(playerIds, season)
    writeShotData(shotData, season, dt)
  }

  private def downloadPlayersShotData(playerIds: Seq[String], season: String): Seq[Shot] = {
    playerIds
      .map(PlayerIdParameter.newParameterValue)
      .flatMap(v => {
        Thread.sleep(1000)
        downloadPlayerShotData(v, SeasonParameter.newParameterValue(season))
      })
  }

  private def downloadPlayerShotData(playerIdParameter: ParameterValue, seasonParameter: ParameterValue): Seq[Shot] = {
    val shotChartEndpoint = ShotChartDetailEndpoint(playerIdParameter)
    ScalabrineClient.getShotChart(shotChartEndpoint).teamGameLog.shots
  }

  private def writeShotData(shots: Seq[Shot], season: String, dt: String): Unit = {
    MySqlClient.createTable(NBATables.raw_shot_data)
    val shotRecords = shots.map(RawShotData.apply(_, season, dt))
    MySqlClient.insertInto(NBATables.raw_shot_data, shotRecords)
  }

  def readShotData(playerId: Option[String]): Seq[RawShotData] = {
    val where = playerId.map(v => Seq(s"playerId = '$v'")).getOrElse(Seq.empty)
    MySqlClient.selectFrom[RawShotData](
      NBATables.raw_shot_data,
      RawShotData.apply,
      where:_*)
  }
}