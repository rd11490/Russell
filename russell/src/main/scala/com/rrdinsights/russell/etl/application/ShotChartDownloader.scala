package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.RawShotData
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.ShotChartDetailEndpoint
import com.rrdinsights.scalabrine.models.Shot
import com.rrdinsights.scalabrine.parameters._

object ShotChartDownloader {

  def downloadAndWritePlayersShotData(playerIds: Seq[(String, String)], dt: String, seasonType: String, season: Option[String] = None): Unit = {
    val shotData = downloadPlayersShotData(playerIds, season, seasonType)
    writeShotData(shotData, dt, seasonType)
  }

  private def downloadPlayersShotData(playerIds: Seq[(String, String)], season: Option[String], seasonType: String): Seq[Shot] = {
    playerIds
      .sortBy(_._2)
      .map(v => (PlayerIdParameter.newParameterValue(v._1), TeamIdParameter.newParameterValue(v._2)))
      .flatMap(v => {
        println(s"PlayerId: ${v._1}, TeamId: ${v._2}")
        Thread.sleep(1500)
        val seasonParam = season.map(s => SeasonParameter.newParameterValue(s)).getOrElse(SeasonParameter.defaultParameterValue)
        val seasonTypeParam = SeasonTypeParameter.newParameterValue(seasonType)

        downloadPlayerShotData(v._1, v._2, seasonParam, seasonTypeParam)
      })
  }

  private def downloadPlayerShotData(playerIdParameter: ParameterValue, teamId: ParameterValue, season: ParameterValue, seasonType: ParameterValue): Seq[Shot] = {
    val shotChartEndpoint = ShotChartDetailEndpoint(playerIdParameter, season = season, teamId = teamId, seasonType = seasonType)
    ScalabrineClient.getShotChart(shotChartEndpoint).teamGameLog.shots
  }

  private def writeShotData(shots: Seq[Shot], dt: String, seasonType: String): Unit = {
    MySqlClient.createTable(NBATables.raw_shot_data)
    val shotRecords = shots.map(RawShotData.apply(_, dt, seasonType))
    MySqlClient.insertInto(NBATables.raw_shot_data, shotRecords)
  }

  def readShotData(whereClauses: String*): Seq[RawShotData] = {
    MySqlClient.selectFrom[RawShotData](
      NBATables.raw_shot_data,
      RawShotData.apply,
      whereClauses:_*)
  }
}