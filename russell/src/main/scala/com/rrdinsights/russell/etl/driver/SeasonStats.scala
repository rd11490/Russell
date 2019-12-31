package com.rrdinsights.russell.etl.driver

import com.rrdinsights.russell.commandline.{CommandLineBase, ForceOption, RunAllOption, SeasonOption}
import com.rrdinsights.russell.etl.application._
import com.rrdinsights.russell.utils.TimeUtils
import org.apache.commons.cli
import org.apache.commons.cli.Options

object SeasonStats {

  def main(strings: Array[String]): Unit = {
    val args = SeasonStatsArguments(strings)
    val season = args.seasonOpt.getOrElse(throw new IllegalArgumentException("A Season must be specified"))
    val dt = TimeUtils.dtNow
    val seasonType = args.seasonType

    if (args.downloadGameLog) {
      GameLogDownloader.downloadAndWriteAllGameLogs(season, dt, seasonType)
    }

    val gameLogs = GameLogDownloader.readGameLogs(season, seasonType)

    if (args.downloadPlayByPlay) {
      PlayByPlayDownloader.downloadAndWriteAllPlayByPlay(gameLogs, season, dt, seasonType, args.force)
    }

//    if (args.downloadRosters) {
//      RosterDownloader.downloadAndWriteAllRosters(season, dt)
//    }

    if (args.downloadAdvancedBoxscore) {
      AdvancedBoxScoreDownloader.downloadAndWriteAllAdvancedBoxScores(gameLogs, season, dt, seasonType, args.force)
    }

    if (args.downloadGameSummaries) {
      BoxScoreSummaryDownloader.downloadAndWriteAllBoxScoreSummaries(gameLogs, dt, args.seasonOpt, seasonType, args.force)
    }

  }

}

private final class SeasonStatsArguments private(args: Array[String])
  extends CommandLineBase(args, "Season Stats") with SeasonOption with RunAllOption with ForceOption {

  override protected def options: Options = super.options
    .addOption(SeasonStatsArguments.GameLogOption)
    .addOption(SeasonStatsArguments.PlayByPlayOption)
    .addOption(SeasonStatsArguments.RosterOption)
    .addOption(SeasonStatsArguments.AdvacnedBoxScoreOption)
    .addOption(SeasonStatsArguments.GameSummaries)

  lazy val downloadGameLog: Boolean = has(SeasonStatsArguments.GameLogOption) || runAll

  lazy val downloadPlayByPlay: Boolean = has(SeasonStatsArguments.PlayByPlayOption) || runAll

  lazy val downloadRosters: Boolean = has(SeasonStatsArguments.RosterOption) || runAll

  lazy val downloadAdvancedBoxscore: Boolean = has(SeasonStatsArguments.AdvacnedBoxScoreOption) || runAll

  lazy val downloadGameSummaries: Boolean = has(SeasonStatsArguments.GameSummaries) || runAll

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