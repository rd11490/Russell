package com.rrdinsights.russell.etl.driver

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.rrdinsights.russell.commandline.{CommandLineBase, RunAllOption, SeasonOption}
import com.rrdinsights.russell.etl.application.{PlayerProfileDownloader, RosterDownloader, ShotChartDownloader}
import com.rrdinsights.scalabrine.parameters.SeasonParameter
import org.apache.commons.cli

object PlayerStats {
  private val Formatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

  def main(strings: Array[String]): Unit = {
    val args = PlayerStatsArguments(strings)
    val playerId = args.playerId
    val dt = LocalDateTime.now().format(Formatter)
    val season = args.season

    if (playerId.isDefined) {
      downloadAndWritePlayerStats(args, dt, playerId.get)
    } else {
      val playersFromRosters = readPlayersFromRosters(season)

      val players = if (args.delta) {
        playersFromRosters.diff(readPlayersFromShotCharts())
      } else {
        playersFromRosters
      }
      downloadAndWritePlayerStats(args, dt, players: _*)

    }
  }

  private def downloadAndWritePlayerStats(args: PlayerStatsArguments, dt: String, playerIds: String*): Unit = {
    val season = args.season
    if (args.downloadShotData) {
      ShotChartDownloader.downloadAndWritePlayersShotData(playerIds, dt, season)
    }
    if (args.playerProfiles) {
      PlayerProfileDownloader.downloadAndWriteAllPlayerProfiles(playerIds, dt)
    }
  }

  private def readPlayersFromRosters(season: Option[String]): Seq[String] = {
    val where = season.map(v => Seq(s"season = '$v'")).getOrElse(Seq.empty)
    RosterDownloader.readPlayerInfo(where)
      .map(_.playerId.toString)
      .distinct
  }

  private def readPlayersFromShotCharts(/*IO*/): Seq[String] = {
    ShotChartDownloader.readShotData()
      .map(_.playerId.toString)
      .distinct
  }
}

private final class PlayerStatsArguments private(args: Array[String])
  extends CommandLineBase(args, "Player Stats") with SeasonOption with RunAllOption {

  override protected def options: cli.Options = super.options
    .addOption(PlayerStatsArguments.PlayerIdOption)
    .addOption(PlayerStatsArguments.ShotDataOption)
    .addOption(PlayerStatsArguments.DeltaOption)
    .addOption(PlayerStatsArguments.PlayerProfileTotals)


  def playerId: Option[String] = valueOf(PlayerStatsArguments.PlayerIdOption)

  lazy val downloadShotData: Boolean = has(PlayerStatsArguments.ShotDataOption) || runAll

  lazy val delta: Boolean = has(PlayerStatsArguments.DeltaOption)

  lazy val playerProfiles: Boolean = has(PlayerStatsArguments.PlayerProfileTotals)

}

private object PlayerStatsArguments {

  val PlayerIdOption: cli.Option =
    new cli.Option(null, "player", true, "The player id you want to collect data for")

  val PlayerProfileTotals: cli.Option =
    new cli.Option(null, "profiles", false, "Download and store all basic player profile totals")

  val ShotDataOption: cli.Option =
    new cli.Option(null, "shot-data", false, "Download and store all teams game logs for a particular season")

  val DeltaOption: cli.Option =
    new cli.Option(null, "delta", false, "Only Download players not already in the shot database")

  def apply(args: Array[String]): PlayerStatsArguments = new PlayerStatsArguments(args)
}
