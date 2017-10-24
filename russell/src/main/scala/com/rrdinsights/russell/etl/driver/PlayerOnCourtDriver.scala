package com.rrdinsights.russell.etl.driver

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.rrdinsights.russell.commandline.{CommandLineBase, SeasonOption}
import com.rrdinsights.russell.etl.application.{BoxScoreSummaryDownloader, PlayByPlayDownloader, PlayersOnCourtDownloader}
import com.rrdinsights.russell.storage.datamodel.{DataModelUtils, PlayersOnCourt, RawGameSummary, RawPlayByPlayEvent}

object PlayerOnCourtDriver {

  private val Formatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

  def main(strings: Array[String]): Unit = {
    val dt = LocalDateTime.now().format(Formatter)
    val args = PlayerOnCourtArguments(strings)
    downloadAndWritePlayerOnCourtForShots(dt, args.season)
  }

  private def downloadAndWritePlayerOnCourtForShots(dt: String, season: Option[String]): Unit = {
    val where = season.map(v => Seq(s"season = $v")).getOrElse(Seq.empty)
    val gameSummaries = BoxScoreSummaryDownloader.readGameSummary(where: _ *)
    val playersOnCourtAtQuarter = gameSummaries.flatMap(downloadPlayersOnCourtAtQuarter(_, dt))
      .groupBy(_.gameId)

    val playByPlay = PlayByPlayDownloader.readPlayByPlay(where:_ *)
      .groupBy(_.gameId)

    val joinedPlayByPlayData = joinPlayByPlayWithPlayersOnCourt(playersOnCourtAtQuarter, playByPlay)

    val playersOnCourt = joinedPlayByPlayData
      .filter(_._2._2.isDefined)
      .map(v => calculatePlayersOnCourt(v._2._1, v._2._2.get))

    //PlayersOnCourtDownloader.writePlayersOnCourt(playersOnCourt)
  }

  def calculatePlayersOnCourt(playByPlay: Seq[RawPlayByPlayEvent], playersOnCourt: Seq[PlayersOnCourt]): Seq[PlayersOnCourt] = {
    //TODO implement
    playersOnCourt
  }

  def joinPlayByPlayWithPlayersOnCourt(playersOnCourt: Map[String, Seq[PlayersOnCourt]], playByPlay: Map[String, Seq[RawPlayByPlayEvent]]): Map[String, (Seq[RawPlayByPlayEvent], Option[Seq[PlayersOnCourt]])] =
    playByPlay.map(v => (v._1, (v._2, playersOnCourt.get(v._1))))

  def downloadPlayersOnCourtAtQuarter(gameSummary: RawGameSummary, dt: String): Seq[PlayersOnCourt] = {
    val gameId = gameSummary.gameId

    val periods = (1 to gameSummary.livePeriod.intValue()).toArray

    periods.flatMap(v => downloadPlayersOnCourtAtStartOfPeriod(gameId, v, dt))
  }

  def downloadPlayersOnCourtAtStartOfPeriod(gameId: String, period: Int, dt: String): Option[PlayersOnCourt] = {
    val time = PlayersOnCourtDownloader.timeFromStartOfGameAtPeriod(period) * 10

    val players = PlayersOnCourtDownloader.downloadPlayersOnCourt(gameId, time)
    if (players.size == 10 && players.slice(0, 4).map(_._2).distinct.size == 1 && players.slice(5, 9).map(_._2).distinct.size == 1) {
      val primaryKey = s"${gameId}_$period"
      Some(PlayersOnCourt(
        primaryKey,
        gameId,
        period,
        players.head._2,
        players.head._1,
        players(1)._1,
        players(2)._1,
        players(3)._1,
        players(4)._1,
        players(5)._2,
        players(5)._1,
        players(6)._1,
        players(7)._1,
        players(8)._1,
        players(9)._1,
        dt,
        DataModelUtils.gameIdToSeason(gameId)))

    }
    else {
      println(s"$gameId-$period")
      println(s"$time")
      println(s"${players.size}")
      println(s"${players.groupBy(_._2).map(v => s"${v._1} - ${v._2.size}").mkString(" | ")}")
      None
    }
  }

}

private final class PlayerOnCourtArguments private(args: Array[String])
  extends CommandLineBase(args, "Season Stats") with SeasonOption

private object PlayerOnCourtArguments {
  def apply(args: Array[String]): PlayerOnCourtArguments = new PlayerOnCourtArguments(args)
}