package com.rrdinsights.russell.etl.application

import java.sql.ResultSet

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{RawGameInfo, ResultSetMapper}
import com.rrdinsights.russell.storage.tables.NBATables

object GameDateMapBuilder {

  def main(strings: Array[String]): Unit = {
    val gameDates = buildGameDateMap()
    writeGameDateMap(gameDates)
  }


  private def buildGameDateMap(/*IO*/): Seq[GameDate] = {
    MySqlClient.selectFrom(NBATables.raw_game_info, RawGameInfo.apply)
      .map(v => GameDate(v.gameId, v.gameId, v.gameDate, v.season))
      .distinct
  }

  private def writeGameDateMap(gameDates: Seq[GameDate]): Unit = {
    MySqlClient.createTable(NBATables.game_dates)
    MySqlClient.insertInto(NBATables.game_dates, gameDates)
  }

}

final case class GameDate(primaryKey: String,
                          gameId: String,
                          gameDate: String,
                          season: String)

object GameDate extends ResultSetMapper {
  def apply(resultSet: ResultSet): GameDate =
    GameDate(
      getString(resultSet, 0),
      getString(resultSet, 1),
      getString(resultSet, 2),
      getString(resultSet, 3))
}