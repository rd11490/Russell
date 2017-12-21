package com.rrdinsights.russell.investigation.shots.expectedshots

import com.rrdinsights.russell.commandline.{CommandLineBase, SeasonOption}
import org.apache.commons.cli

final class ExpectedPointsArguments private(args: Array[String])
  extends CommandLineBase(args, "ExpectedPointsArguments") with SeasonOption {

  override protected def options: cli.Options = super.options
    .addOption(ExpectedPointsArguments.OffenseOption)
    .addOption(ExpectedPointsArguments.DefenseOption)
    .addOption(ExpectedPointsArguments.ZoneOption)

  lazy val offense: Boolean = has(ExpectedPointsArguments.OffenseOption)

  lazy val defense: Boolean = has(ExpectedPointsArguments.DefenseOption)

  lazy val zoned: Boolean = has(ExpectedPointsArguments.ZoneOption)
}

object ExpectedPointsArguments {

  def apply(args: Array[String]): ExpectedPointsArguments = new ExpectedPointsArguments(args)

  val OffenseOption: cli.Option =
    new cli.Option("o", "offense", false, "Calculate Offense ExpectedPoints")

  val DefenseOption: cli.Option =
    new cli.Option("d", "defense", false, "Calculate Defense ExpectedPoints")

  val ZoneOption: cli.Option =
    new cli.Option("z", "zone", false, "Calculate expected points per zone")

}
