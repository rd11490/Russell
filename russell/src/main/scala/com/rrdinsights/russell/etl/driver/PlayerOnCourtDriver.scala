package com.rrdinsights.russell.etl.driver

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.rrdinsights.russell.commandline.{CommandLineBase, SeasonOption}
import com.rrdinsights.russell.etl.application.{BoxScoreSummaryDownloader, PlayByPlayDownloader, PlayersOnCourtDownloader}
import com.rrdinsights.russell.investigation.PlayByPlayParser
import com.rrdinsights.russell.storage.datamodel._
import org.apache.commons.cli
import org.apache.commons.cli.Options

object PlayerOnCourtDriver {

  private val Formatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

  def main(strings: Array[String]): Unit = {
    val dt = LocalDateTime.now().format(Formatter)
    val args = PlayerOnCourtArguments(strings)
    if (args.downloadPlayersAtStartOfPeriod) {
      downloadAndWritePlayerOnCourtForShots(dt, args.season)
    }

    if (args.parsePlayByPlayForPlayers) {
      parsePlayersOnCourt(dt, args.season)
    }
  }

  private def downloadAndWritePlayerOnCourtForShots2(dt: String, season: Option[String]): Unit = {
    val where = season.map(v => Seq(s"season = '$v'")).getOrElse(Seq.empty)
    val playByPlay = PlayByPlayDownloader.readPlayByPlay(where: _ *)
    val playersOnCourtAtQuarter = downloadPlayersOnCourtAtQuarter2(playByPlay, dt)
    PlayersOnCourtDownloader.writePlayersOnCourtAtPeriod(playersOnCourtAtQuarter)
  }

  private def downloadAndWritePlayerOnCourtForShots(dt: String, season: Option[String]): Unit = {
    val where = season.map(v => Seq(s"season = '$v'")).getOrElse(Seq.empty)
    val gameSummaries = BoxScoreSummaryDownloader.readGameSummary(where: _ *)
    val playersOnCourtAtQuarter = gameSummaries.flatMap(downloadPlayersOnCourtAtQuarter(_, dt))
    PlayersOnCourtDownloader.writePlayersOnCourtAtPeriod(playersOnCourtAtQuarter)
  }

  def parsePlayersOnCourt(dt: String, season: Option[String]): Unit = {
    val where = season.map(v => Seq(s"season = '$v'")).getOrElse(Seq.empty)

    val playersOnCourtAtQuarter = PlayersOnCourtDownloader.readPlayersOnCourtAtPeriod(where:_ *)
    .groupBy(_.gameId)

    val playByPlay = PlayByPlayDownloader.readPlayByPlay(where:_ *)
      .groupBy(_.gameId)

    val joinedPlayByPlayData = joinPlayByPlayWithPlayersOnCourt(playersOnCourtAtQuarter, playByPlay)

    val playersOnCourt = joinedPlayByPlayData
      .flatMap(v => v._2._2.map(p => calculatePlayersOnCourt(v._2._1, p, dt)))
      .flatten
      .toSeq

    PlayersOnCourtDownloader.writePlayersOnCourt(playersOnCourt)

  }

  def calculatePlayersOnCourt(playByPlay: Seq[RawPlayByPlayEvent], playersOnCourt: Seq[PlayersOnCourt], dt: String): Seq[PlayersOnCourt] = {
    val parser = new PlayByPlayParser(playByPlay, playersOnCourt, dt)
    parser.run()
  }

  def joinPlayByPlayWithPlayersOnCourt(playersOnCourt: Map[String, Seq[PlayersOnCourt]], playByPlay: Map[String, Seq[RawPlayByPlayEvent]]): Map[String, (Seq[RawPlayByPlayEvent], Option[Seq[PlayersOnCourt]])] =
    playByPlay.map(v => (v._1, (v._2, playersOnCourt.get(v._1))))

  def downloadPlayersOnCourtAtQuarter(gameSummary: RawGameSummary, dt: String): Seq[PlayersOnCourt] = {
    val gameId = gameSummary.gameId

    val periods = (1 to gameSummary.livePeriod.intValue()).toArray

    periods.flatMap(v => PlayersOnCourtDownloader.downloadPlayersOnCourtAtStartOfPeriod(gameId, v, dt))
  }

  private def downloadPlayersOnCourtAtQuarter2(playByPlay: Seq[RawPlayByPlayEvent], dt: String): Seq[PlayersOnCourt] = {
    val playByPlayPerGame = playByPlay
      .groupBy(_.gameId)
      .flatMap(v => getFirstEventOfQuarters(v._2))

    playByPlayPerGame.flatMap(v => PlayersOnCourtDownloader.downloadPlayersOnCourtAtEvent(v, dt))

    Seq.empty
  }

  private def getFirstEventOfQuarters(playByPlay: Seq[RawPlayByPlayEvent]): Seq[RawPlayByPlayEvent] = {
    val playByPlayWithIndex = playByPlay.zipWithIndex

    val indicies = playByPlayWithIndex
      .filter(v => PlayByPlayEventMessageType.valueOf(v._1.playType) == PlayByPlayEventMessageType.StartOfPeriod)
      .map(v => {
        if (v._1.period == 1 || v._1.period > 4) v._2 + 2 else v._2 + 1
      })

    playByPlayWithIndex
      .filter(v => indicies.contains(v._2))
      .map(_._1)
  }

}

private final class PlayerOnCourtArguments private(args: Array[String])
  extends CommandLineBase(args, "Season Stats") with SeasonOption {

  override protected def options: Options = super.options
    .addOption(PlayerOnCourtArguments.PlayersOnCourtAtPeriod)
    .addOption(PlayerOnCourtArguments.ParsePlayByPlay)

  def downloadPlayersAtStartOfPeriod: Boolean = has(PlayerOnCourtArguments.PlayersOnCourtAtPeriod)

  def parsePlayByPlayForPlayers: Boolean = has(PlayerOnCourtArguments.ParsePlayByPlay)
}

private object PlayerOnCourtArguments {
  def apply(args: Array[String]): PlayerOnCourtArguments = new PlayerOnCourtArguments(args)

  val PlayersOnCourtAtPeriod: cli.Option =
    new cli.Option(null, "download", false, "Download and store all players at the start of each period")

  val ParsePlayByPlay: cli.Option =
    new cli.Option(null, "parse", false, "Parse the play by play to determine who is on the court at all times")

}