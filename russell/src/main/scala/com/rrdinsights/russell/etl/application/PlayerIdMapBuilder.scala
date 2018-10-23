package com.rrdinsights.russell.etl.application

import java.sql.ResultSet

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{
  RawPlayerBoxScoreAdvanced,
  ResultSetMapper
}
import com.rrdinsights.russell.storage.tables.NBATables
import org.json4s.jackson.Serialization.write
import org.json4s.{DefaultFormats, Formats}

object PlayerIdMapBuilder {

  def main(strings: Array[String]): Unit = {
    val players = buildPlayerMap()
    writePlayerMap(players)
  }

  private def buildPlayerMap( /*IO*/ ): Seq[PlayerInfo] = {
    MySqlClient
      .selectFrom(
        NBATables.raw_player_box_score_advanced,
        RawPlayerBoxScoreAdvanced.apply
      )
      .map(
        v =>
          PlayerInfo(
            s"${v.playerId}",
            v.playerId,
            v.playerName
        )
      )
      .distinct
  }

  private def writePlayerMap(players: Seq[PlayerInfo]): Unit = {
    MySqlClient.createTable(NBATables.player_info)
    MySqlClient.insertInto(NBATables.player_info, players)
  }

}

final case class PlayerInfo(
    primaryKey: String,
    playerId: Integer,
    playerName: String
)

object PlayerInfo extends ResultSetMapper {

  private implicit val formats: Formats = DefaultFormats
  def apply(resultSet: ResultSet): PlayerInfo =
    PlayerInfo(
      getString(resultSet, 0),
      getInt(resultSet, 1),
      getString(resultSet, 2)
    )

  def toJson(playerInfo: Seq[PlayerInfo]): String = write(playerInfo)
}
