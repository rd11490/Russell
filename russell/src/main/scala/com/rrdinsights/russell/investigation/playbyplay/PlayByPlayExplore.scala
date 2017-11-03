package com.rrdinsights.russell.investigation.playbyplay

import com.rrdinsights.russell.etl.application.{PlayByPlayDownloader, PlayersOnCourtDownloader}
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.tables.NBATables

object PlayByPlayExplore {
  /**
    * This object is just for playing with play by play data to help with production tasks
    *
    */
  def main(strings: Array[String]): Unit = {
    val playByPlay = PlayByPlayDownloader
      .readPlayByPlay("gameId = '0021600001'")
      .sortBy(_.eventNumber)

    val playersOnCourtAtQuarter = PlayersOnCourtDownloader.readPlayersOnCourtAtPeriod("gameId = '0021600001'")

    val parser = new PlayByPlayParser(playByPlay, playersOnCourtAtQuarter, null)
    val results = parser.run().sortBy(_.eventNumber)

    MySqlClient.createTable(NBATables.players_on_court_test)
    MySqlClient.insertInto(NBATables.players_on_court_test, results)

  }


}
