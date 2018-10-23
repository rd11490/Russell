package com.rrdinsights.russell.investigation.playbyplay

import com.rrdinsights.russell.commandline.{CommandLineBase, SeasonOption}
import com.rrdinsights.russell.etl.application.{PlayByPlayDownloader, PlayersOnCourtDownloader}
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{PlayByPlayWithLineup, PlayersOnCourt, RawPlayByPlayEvent}
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.{MapJoin, TimeUtils}

object PlayByPlayLineupJoiner {
  /**
    * This object is just for playing with play by play data to help with production tasks
    *
    */
  def main(strings: Array[String]): Unit = {

    val args = PlayByPlayLineupArguments(strings)
    val seasonWhere: Seq[String] = args.seasonOpt.map(v => Seq(s"season = '$v'")).getOrElse(Seq.empty)
    val where = seasonWhere ++ Seq(s"seasonType = '${args.seasonType}'")
    val dt = TimeUtils.dtNow

    val playByPlay = PlayByPlayDownloader
      .readPlayByPlay(where:_*)

    val playersOnCourt = PlayersOnCourtDownloader.readPlayersOnCourt(where:_*)


    val playByPlayWithLineups = joinEventWithLineup(playByPlay, playersOnCourt)
      .map(v => PlayByPlayWithLineup(v._1, v._2, dt))

    writePlayByPlay(playByPlayWithLineups)

  }

  private def joinEventWithLineup(playByPlay: Seq[RawPlayByPlayEvent], lineups: Seq[PlayersOnCourt]): Seq[(RawPlayByPlayEvent, PlayersOnCourt)] = {
    val playByPlayMap = playByPlay.map(v => ((v.gameId, v.eventNumber), v)).toMap
    val linesupMap = lineups.map(v => ((v.gameId, v.eventNumber), v)).toMap

    MapJoin.join(playByPlayMap, linesupMap)
  }


  private def writePlayByPlay(pbp: Seq[PlayByPlayWithLineup]): Unit = {
    MySqlClient.createTable(NBATables.play_by_play_with_lineup)
    MySqlClient.insertInto(NBATables.play_by_play_with_lineup, pbp)
  }


}

private final class PlayByPlayLineupArguments private(args: Array[String])
  extends CommandLineBase(args, "Play By Play with Lineup") with SeasonOption {

}

private object PlayByPlayLineupArguments {

  def apply(args: Array[String]): PlayByPlayLineupArguments = new PlayByPlayLineupArguments(args)

}