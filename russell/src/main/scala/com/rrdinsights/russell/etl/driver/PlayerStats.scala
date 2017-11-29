package com.rrdinsights.russell.etl.driver

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.rrdinsights.russell.commandline.{CommandLineBase, RunAllOption, SeasonOption}
import com.rrdinsights.russell.etl.application.{RosterDownloader, ShotChartDownloader}
import org.apache.commons.cli

object PlayerStats {
  private val Formatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

  def main(strings: Array[String]): Unit = {
    val args = PlayerStatsArguments(strings)
    val playerId = args.playerId
    val season = args.season
    val dt = LocalDateTime.now().format(Formatter)

    if (playerId.isDefined) {
      downloadAndWritePlayerStats(season, dt, playerId.get)
    } else {
      val playersFromRosters = readPlayersFromRosters(season)

      val players = if (args.delta) {
        playersFromRosters.diff(readPlayersFromShotCharts())
      } else {
        playersFromRosters
      }
      downloadAndWritePlayerStats(season, dt, players: _*)

    }
  }

  private def downloadAndWritePlayerStats(season: Option[String], dt: String, playerIds: String*): Unit = {
    ShotChartDownloader.downloadAndWritePlayersShotData(playerIds, dt, season)
  }

  private def readPlayersFromRosters(season: Option[String]): Seq[String] = {
    val where = season.map(v => Seq(s"seasonParameter = '$v'")).getOrElse(Seq.empty)
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

  def playerId: Option[String] = valueOf(PlayerStatsArguments.PlayerIdOption)

  lazy val downloadShotData: Boolean = has(PlayerStatsArguments.ShotDataOption) || runAll

  lazy val delta: Boolean = has(PlayerStatsArguments.DeltaOption)

}

private object PlayerStatsArguments {

  val PlayerIdOption: cli.Option =
    new cli.Option(null, "player", true, "The season you want to extract games from in the form of yyyy-yy (2016-17)")

  val ShotDataOption: cli.Option =
    new cli.Option(null, "shot-data", false, "Download and store all teams game logs for a particular season")

  val DeltaOption: cli.Option =
    new cli.Option(null, "delta", false, "Only Download players not already in the shot database")

  def apply(args: Array[String]): PlayerStatsArguments = new PlayerStatsArguments(args)
}
