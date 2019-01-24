package com.rrdinsights.russell.etl.driver

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.rrdinsights.russell.commandline.{
  CommandLineBase,
  RunAllOption,
  SeasonOption
}
import com.rrdinsights.russell.etl.application.{PlayerInfo, TeamInfo}
import com.rrdinsights.russell.etl.shotsiteclient.ShotSiteClient
import com.rrdinsights.russell.investigation.PlayerMapper
import com.rrdinsights.russell.investigation.shots.ShotUtils
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.RealAdjustedFourFactors
import com.rrdinsights.russell.storage.tables.NBATables
import org.apache.commons.cli
import org.apache.commons.cli.Options

object ShotSitePutter {
  private val Formatter: DateTimeFormatter =
    DateTimeFormatter.ISO_LOCAL_DATE_TIME

  def main(strings: Array[String]): Unit = {
    val dt = LocalDateTime.now().format(Formatter)
    val args = ShotSitePutterArguments(strings)

    val where =
      Seq(s"season = '${args.season}'", s"seasonType = '${args.seasonType}'")

    if (args.transferShots) {
      val shots = ShotUtils.readShotsWithPlayers(where: _*)

      shots.grouped(1000).foreach(ShotSiteClient.postShots)
    }

    if (args.transferPlayers) {
      val players =
        MySqlClient.selectFrom(NBATables.player_info, PlayerInfo.apply)

      players.grouped(1000).foreach(ShotSiteClient.postPlayers)
    }

    if (args.transferTeams) {
      val teams = MySqlClient.selectFrom(NBATables.team_info, TeamInfo.apply)

      teams.grouped(1000).foreach(ShotSiteClient.postTeams)
    }

    // TODO: ADD SEASON TYPE
    if (args.transferFourFactors) {
      val fourFactors = MySqlClient
        .selectFrom(NBATables.real_adjusted_four_factors,
                    RealAdjustedFourFactors.apply,
                    s"season = '${args.season}'")
        .map(v =>
          v.toRealAdjustedFourFactorsForSite(
            PlayerMapper.lookupTeam(v.playerId, v.season)))

      fourFactors.grouped(1000).foreach(ShotSiteClient.postFourFactors)
    }

  }

}

private final class ShotSitePutterArguments private (args: Array[String])
    extends CommandLineBase(args, "Season Stats")
    with SeasonOption
    with RunAllOption {

  override protected def options: Options =
    super.options
      .addOption(ShotSitePutterArguments.TeamsOption)
      .addOption(ShotSitePutterArguments.PlayersOption)
      .addOption(ShotSitePutterArguments.ShotsOption)
      .addOption(ShotSitePutterArguments.FourFactorsOption)

  lazy val transferTeams
    : Boolean = has(ShotSitePutterArguments.TeamsOption) || runAll

  lazy val transferPlayers
    : Boolean = has(ShotSitePutterArguments.PlayersOption) || runAll

  lazy val transferShots
    : Boolean = has(ShotSitePutterArguments.ShotsOption) || runAll

  lazy val transferFourFactors: Boolean = has(
    ShotSitePutterArguments.FourFactorsOption) || runAll

}

private object ShotSitePutterArguments {
  def apply(args: Array[String]): ShotSitePutterArguments =
    new ShotSitePutterArguments(args)

  val TeamsOption: cli.Option =
    new cli.Option(null, "teams", false, "Transfer team info to the shot site")

  val PlayersOption: cli.Option =
    new cli.Option(null,
                   "players",
                   false,
                   "Transfer player info to the shot site")

  val ShotsOption: cli.Option =
    new cli.Option(null, "shots", false, "Transfer shots to the shot site")

  val FourFactorsOption: cli.Option =
    new cli.Option(null,
                   "four-factors",
                   false,
                   "Transfer four factors to the shot site")

}
