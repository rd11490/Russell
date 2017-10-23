package com.rrdinsights.russell.etl.driver

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.rrdinsights.russell.etl.application._
import com.rrdinsights.russell.commandline.{CommandLineBase, SeasonOption}
import org.apache.commons.cli
import org.apache.commons.cli.Options

object SeasonStats {

  private val Formatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

  def main(strings: Array[String]): Unit = {
    val args = SeasonStatsArguments(strings)
    val season = args.season.getOrElse(throw new IllegalArgumentException("A Season must be specified"))
    val dt = LocalDateTime.now().format(Formatter)

    if (args.downloadGameLog) {
      GameLogDownloader.downloadAndWriteAllGameLogs(season, dt)
    }

    val gameLogs = GameLogDownloader.readGameLogs(season)

    if (args.downloadPlayByPlay) {
      PlayByPlayDownloader.downloadAndWriteAllPlayByPlay(gameLogs, season, dt)
    }

    if (args.downloadRosters) {
      RosterDownloader.downloadAndWriteAllRosters(season, dt)
    }

    if (args.downloadAdvancedBoxscore) {
      AdvancedBoxScoreDownloader.downloadAndWriteAllAdvancedBoxScores(gameLogs, season, dt)
    }

    if (args.downloadGameSummaries) {
      BoxScoreSummaryDownloader.downloadAndWriteAllBoxScoreSummaries(gameLogs, dt, args.season)
    }

  }

}

private final class SeasonStatsArguments private(args: Array[String])
  extends CommandLineBase(args, "Season Stats") with SeasonOption {

  override protected def options: Options = super.options
    .addOption(SeasonStatsArguments.GameLogOption)
    .addOption(SeasonStatsArguments.PlayByPlayOption)
    .addOption(SeasonStatsArguments.RosterOption)
    .addOption(SeasonStatsArguments.AdvacnedBoxScoreOption)
    .addOption(SeasonStatsArguments.GameSummaries)

  def downloadGameLog: Boolean = has(SeasonStatsArguments.GameLogOption)

  def downloadPlayByPlay: Boolean = has(SeasonStatsArguments.PlayByPlayOption)

  def downloadRosters: Boolean = has(SeasonStatsArguments.RosterOption)

  def downloadAdvancedBoxscore: Boolean = has(SeasonStatsArguments.AdvacnedBoxScoreOption)

  def downloadGameSummaries: Boolean = has(SeasonStatsArguments.GameSummaries)

}

private object SeasonStatsArguments {

  val GameLogOption: cli.Option =
    new cli.Option(null, "game-log", false, "Download and store all teams game logs for a particular season")

  val PlayByPlayOption: cli.Option =
    new cli.Option(null, "play-by-play", false, "Download and store all play by play data for a given season")

  val RosterOption: cli.Option =
    new cli.Option(null, "roster", false, "Download and store all rosters for a given season")

  val AdvacnedBoxScoreOption: cli.Option =
    new cli.Option(null, "advanced-box-score", false, "Download and store all advanced box scores for a given season")

  val GameSummaries: cli.Option =
    new cli.Option(null, "game-summary", false, "Download and store all game summaries for a given season")


  def apply(args: Array[String]): SeasonStatsArguments = new SeasonStatsArguments(args)
}