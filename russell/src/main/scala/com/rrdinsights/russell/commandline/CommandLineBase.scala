package com.rrdinsights.russell.commandline

import com.rrdinsights.russell.utils.SmartOption
import org.apache.commons.cli
import org.apache.commons.cli.{CommandLine, DefaultParser, HelpFormatter, MissingOptionException, Options}

/** Base class for specifying and processing command line arguments.
  *
  * Extend this class when building command line args for jobs
  */
class CommandLineBase(args: Array[String], helpTitle: String) {

  protected final val cmd: CommandLine = new DefaultParser().parse(this.options, args, false)

  protected def options: cli.Options =
    new Options().addOption(CommandLineBase.HelpOption)

  final def help: Boolean =
    this.has(CommandLineBase.HelpOption)

  final def printHelp(): Unit = new HelpFormatter().printHelp(helpTitle, this.options)

  protected final def has(option: cli.Option): Boolean =
    cmd.hasOption(CommandLineBase.optionKey(option))

  protected final def getParsedOptionValue(opt: String): Object = cmd.getParsedOptionValue(opt)

  protected final def getOptions: Array[cli.Option] = cmd.getOptions

  protected final def valuesOf(option: cli.Option): Seq[String] =
    rawValuesOf(option).map(_.trim).filterNot(_.isEmpty).toSeq

  protected final def rawValuesOf(option: cli.Option): Array[String] =
    scala.Option(cmd.getOptionValues(CommandLineBase.optionKey(option)))
      .getOrElse(Array.empty)

  protected final def valueOf(option: cli.Option): scala.Option[String] =
    this.valuesOf(option).headOption

  @throws[MissingOptionException]("The option is not present in the command line")
  protected final def requiredValueOf(option: cli.Option): String = this.valueOf(option)
    .getOrElse(throw newMissingOptionException(option))

  protected final def newMissingOptionException(requiredOptions: cli.Option*): MissingOptionException = {
    import scala.collection.JavaConverters.seqAsJavaListConverter
    new MissingOptionException(requiredOptions.asJava)
  }

  protected def asIntOpt(opt: cli.Option): scala.Option[Int] = this.valueOf(opt).map(_.toInt)
}

object CommandLineBase {

  private val HelpOption: cli.Option = new cli.Option("h", "help", false, "Prints this Help")

  private def optionKey(option: cli.Option): String =
    SmartOption(option.getOpt)
      .getOrElse(option.getLongOpt)

  def fromSystemProperties(property: String): scala.Option[String] = {
    val properties = sys.props
    properties.get(property)
  }
}