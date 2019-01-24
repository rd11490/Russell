package com.rrdinsights.russell.etl.driver

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.rrdinsights.russell.commandline.{CommandLineBase, RunAllOption, SeasonOption}
import com.rrdinsights.russell.etl.application.{AdvancedBoxScoreDownloader, PlayerProfileDownloader, RosterDownloader, ShotChartDownloader}
import org.apache.commons.cli

object PlayerStats {
  private val Formatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

  def main(strings: Array[String]): Unit = {
    val args = PlayerStatsArguments(strings)
    val playerId = args.playerId
    val teamId = args.teamId
    val dt = LocalDateTime.now().format(Formatter)
    val season = args.seasonOpt

    if (playerId.isDefined && teamId.isDefined) {
      downloadAndWritePlayerStats(args, dt, (playerId.get, teamId.get))
    } else {
      val playersFromRosters = readPlayersFromRosters(season, args.seasonType)

      val players = if (args.delta) {
        playersFromRosters.diff(readPlayersFromShotCharts())
      } else {
        playersFromRosters
      }
      downloadAndWritePlayerStats(args, dt, players: _*)

    }
  }

  private def downloadAndWritePlayerStats(args: PlayerStatsArguments, dt: String, playerIds: (String, String)*): Unit = {
    val season = args.seasonOpt
    val seasonType = args.seasonType
    if (args.downloadShotData) {
      ShotChartDownloader.downloadAndWritePlayersShotData(playerIds, dt, seasonType, season)
    }
    if (args.playerProfiles) {
      PlayerProfileDownloader.downloadAndWriteAllPlayerProfiles(playerIds.map(_._1), dt)
    }
  }

  private def readPlayersFromRosters(season: Option[String], seasonType: String): Seq[(String, String)] = {
    val where = season.map(v =>  Seq(s"season = '$v'", s"seasonType = '$seasonType'")).getOrElse(Seq.empty)
    val boxScores = AdvancedBoxScoreDownloader.readPlayerStats(where:_*)
    boxScores
      .map(v => (v.playerId.toString, v.teamId.toString))
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
    .addOption(PlayerStatsArguments.TeamIdOption)


  def playerId: Option[String] = valueOf(PlayerStatsArguments.PlayerIdOption)

  def teamId: Option[String] = valueOf(PlayerStatsArguments.TeamIdOption)


  lazy val downloadShotData: Boolean = has(PlayerStatsArguments.ShotDataOption) || runAll

  lazy val delta: Boolean = has(PlayerStatsArguments.DeltaOption)

  lazy val playerProfiles: Boolean = has(PlayerStatsArguments.PlayerProfileTotals) || runAll

}

private object PlayerStatsArguments {

  val PlayerIdOption: cli.Option =
    new cli.Option(null, "player", true, "The player id you want to collect data for")

  val TeamIdOption: cli.Option =
    new cli.Option(null, "team", true, "The team id you want to collect data for")

  val PlayerProfileTotals: cli.Option =
    new cli.Option(null, "profiles", false, "Download and store all basic player profile totals")

  val ShotDataOption: cli.Option =
    new cli.Option(null, "shot-data", false, "Download and store all teams game logs for a particular season")

  val DeltaOption: cli.Option =
    new cli.Option(null, "delta", false, "Only Download players not already in the shot database")

  def apply(args: Array[String]): PlayerStatsArguments = new PlayerStatsArguments(args)
}
