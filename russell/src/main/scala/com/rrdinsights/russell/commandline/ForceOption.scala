package com.rrdinsights.russell.commandline

import org.apache.commons.cli

trait ForceOption extends CommandLineBase {

  override protected def options: cli.Options = super.options
    .addOption(ForceOption.Force)

  def force: Boolean = has(ForceOption.Force)
}

object ForceOption {
  val Force: cli.Option =
    new cli.Option(null, "force", false, "force an operation")
}