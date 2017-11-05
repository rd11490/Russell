package com.rrdinsights.russell.commandline

import org.apache.commons.cli


trait RunAllOption extends CommandLineBase {

  override protected def options: cli.Options = super.options
    .addOption(RunAllOption.RunAll)

  lazy val runAll: Boolean = has(RunAllOption.RunAll)
}


object RunAllOption {
  val RunAll: cli.Option =
    new cli.Option(null, "run-all", true, "run all sections")
}