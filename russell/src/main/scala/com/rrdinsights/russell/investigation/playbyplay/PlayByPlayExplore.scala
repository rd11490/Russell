package com.rrdinsights.russell.investigation.playbyplay

import com.rrdinsights.russell.etl.application.{PlayByPlayDownloader, PlayersOnCourtDownloader}
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{PlayersOnCourt, RawPlayByPlayEvent}
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.MapJoin

object PlayByPlayExplore {
  /**
    * This object is just for playing with play by play data to help with production tasks
    *
    */
  def main(strings: Array[String]): Unit = {
    val playByPlay = PlayByPlayDownloader
      .readPlayByPlay("gameId = '0021600001'")
      .sortBy(_.eventNumber)

    val playersOnCourt = PlayersOnCourtDownloader.readPlayersOnCourt("gameId = '0021600001'")


    val playByPlayWithLineups = joinEventWithLineup(playByPlay, playersOnCourt)

  }

  private def joinEventWithLineup(playByPlay: Seq[RawPlayByPlayEvent], lineups: Seq[PlayersOnCourt]): Seq[(RawPlayByPlayEvent,PlayersOnCourt)] = {
    val playByPlayMap = playByPlay.map(v => ((v.gameId, v.eventNumber), v)).toMap
    val linesupMap = lineups.map(v => ((v.gameId, v.eventNumber), v)).toMap

    MapJoin.join(playByPlayMap, linesupMap)
  }


}
