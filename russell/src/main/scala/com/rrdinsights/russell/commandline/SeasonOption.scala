package com.rrdinsights.russell.commandline

import org.apache.commons.cli

trait SeasonOption extends CommandLineBase {

  override protected def options: cli.Options = super.options
    .addOption(SeasonOption.SeasonOption)
    .addOption(SeasonOption.SeasonTypeOption)

  def seasonOpt: Option[String] = valueOf(SeasonOption.SeasonOption)

  def season: String = seasonOpt
    .getOrElse(throw new IllegalArgumentException("Season must be provided"))

  def seasonTypeOpt: Option[String] = valueOf(SeasonOption.SeasonTypeOption)


  def seasonType: String = seasonTypeOpt.getOrElse("Regular Season")

}

object SeasonOption {
  val SeasonOption: cli.Option =
    new cli.Option(null, "season", true, "The season you want to extract games from in the form of yyyy-yy (2016-17)")

  val SeasonTypeOption: cli.Option =
    new cli.Option(null, "seasonType", true, "The season type (Regular Season, Playoffs)")
}