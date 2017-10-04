package com.rrdinsights.russell.etl.driver

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.rrdinsights.russell.etl.application.{AdvancedBoxScoreDownloader, RosterDownloader, ShotChartDownloader}
import org.apache.commons.cli
import com.rrdinsights.russell.commandline.{CommandLineBase, SeasonOption}
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.scalabrine.parameters.{ParameterValue, PlayerIdParameter}

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
      val playersFromRosteres = readPlayersFromRosters(season)

      val players = if (args.delta) {
        playersFromRosteres.diff(readPlayersFromShotCharts())
      } else {
        playersFromRosteres
      }
      downloadAndWritePlayerStats(season, dt, players: _*)

    }
  }

  private def downloadAndWritePlayerStats(season: Option[String], dt: String, playerIds: String*): Unit = {
    val seasonStr = season.getOrElse("")
    ShotChartDownloader.downloadAndWritePlayersShotData(playerIds, dt, seasonStr)
  }

  private def readPlayersFromRosters(season: Option[String]): Seq[String] = {
    val where = season.map(v => Seq(s"Season = '$v'")).getOrElse(Seq.empty)
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
  extends CommandLineBase(args, "Player Stats") with SeasonOption {

  override protected def options: cli.Options = super.options
    .addOption(PlayerStatsArguments.PlayerIdOption)
    .addOption(PlayerStatsArguments.ShotDataOption)
    .addOption(PlayerStatsArguments.DeltaOption)

  def playerId: Option[String] = valueOf(PlayerStatsArguments.PlayerIdOption)

  def downloadShotData: Boolean = has(PlayerStatsArguments.ShotDataOption)

  def delta: Boolean = has(PlayerStatsArguments.DeltaOption)

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
