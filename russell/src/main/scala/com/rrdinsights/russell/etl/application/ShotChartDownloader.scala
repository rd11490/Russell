package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.RawShotData
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.ShotChartDetailEndpoint
import com.rrdinsights.scalabrine.models.Shot
import com.rrdinsights.scalabrine.parameters.{ParameterValue, PlayerIdParameter, SeasonParameter, TeamIdParameter}

object ShotChartDownloader {

  def downloadAndWritePlayersShotData(playerIds: Seq[(String, String)], dt: String, season: Option[String] = None): Unit = {
    val shotData = downloadPlayersShotData(playerIds, season)
    writeShotData(shotData, dt)
  }

  private def downloadPlayersShotData(playerIds: Seq[(String, String)], season: Option[String]): Seq[Shot] = {
    playerIds
      .sortBy(_._2)
      .map(v => (PlayerIdParameter.newParameterValue(v._1), TeamIdParameter.newParameterValue(v._2)))
      .flatMap(v => {
        println(s"PlayerId: ${v._1}, TeamId: ${v._2}")
        Thread.sleep(1500)
        val seasonParam = season.map(s => SeasonParameter.newParameterValue(s)).getOrElse(SeasonParameter.defaultParameterValue)
        downloadPlayerShotData(v._1, v._2, seasonParam)
      })
  }

  private def downloadPlayerShotData(playerIdParameter: ParameterValue, teamId: ParameterValue, season: ParameterValue): Seq[Shot] = {
    val shotChartEndpoint = ShotChartDetailEndpoint(playerIdParameter, season = season, teamId = teamId)
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