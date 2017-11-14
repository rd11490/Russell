package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.RawShotData
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.ShotChartDetailEndpoint
import com.rrdinsights.scalabrine.models.Shot
import com.rrdinsights.scalabrine.parameters.{ParameterValue, PlayerIdParameter, SeasonParameter}

object ShotChartDownloader {

  def downloadAndWritePlayersShotData(playerIds: Seq[String], dt: String): Unit = {
    val shotData = downloadPlayersShotData(playerIds)
    writeShotData(shotData, dt)
  }

  private def downloadPlayersShotData(playerIds: Seq[String]): Seq[Shot] = {
    playerIds
      .map(PlayerIdParameter.newParameterValue)
      .flatMap(v => {
        Thread.sleep(1000)
        downloadPlayerShotData(v)
      })
  }

  private def downloadPlayerShotData(playerIdParameter: ParameterValue): Seq[Shot] = {
    val shotChartEndpoint = ShotChartDetailEndpoint(playerIdParameter)
    ScalabrineClient.getShotChart(shotChartEndpoint).teamGameLog.shots
  }

  private def writeShotData(shots: Seq[Shot], dt: String): Unit = {
    MySqlClient.createTable(NBATables.raw_shot_data)
    val shotRecords = shots.map(RawShotData.apply(_, dt))
    MySqlClient.insertInto(NBATables.raw_shot_data, shotRecords)
  }

  def readShotData(whereClauses: String*): Seq[RawShotData] = {
    MySqlClient.selectFrom[RawShotData](
      NBATables.raw_shot_data,
      RawShotData.apply,
      whereClauses:_*)
  }
}