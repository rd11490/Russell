package com.rrdinsights.russell.commandline

import org.apache.commons.cli

trait SeasonOption extends CommandLineBase {

  override protected def options: cli.Options = super.options
    .addOption(SeasonOption.SeasonOption)

  lazy val season: Option[String] = valueOf(SeasonOption.SeasonOption)
}

object SeasonOption {
  val SeasonOption: cli.Option =
    new cli.Option(null, "season", true, "The season you want to extract games from in the form of yyyy-yy (2016-17)")
}