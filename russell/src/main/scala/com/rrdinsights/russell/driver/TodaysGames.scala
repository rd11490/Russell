package com.rrdinsights.russell.driver

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.rrdinsights.russell.commandline.CommandLineBase
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.ScoreboardEndpoint
import com.rrdinsights.scalabrine.models.{ConferenceStanding, GameHeader, Scoreboard}
import com.rrdinsights.scalabrine.parameters.GameDateParameter
import org.apache.commons.cli
import org.apache.commons.cli.Options

object TodaysGames {
  def main(strings: Array[String]): Unit = {
    val args = Arguments(strings)
    val todaysGames = checkTodaysScoreboard(args.date)
    writeScoreboard(todaysGames.gameHeader)
    writeStandings(todaysGames.westernConferenceStandings ++ todaysGames.easternConferenceStandings)
  }

  private def checkTodaysScoreboard(date: String): Scoreboard = {
    val gameDate = GameDateParameter.newParameterValue(date)
    val scoreboardEndpoint = ScoreboardEndpoint(gameDate)
    ScalabrineClient.getScoreboard(scoreboardEndpoint).scoreboard
  }

  private def checkHeadersExist(date: String, headers: Seq[GameHeader]): Boolean = {
    //ToDo check to see if any of these values have already been written to SQL, if so do not write
    //See if MySQL already does this for you... unique index on gameId?
    false
  }

  private def writeScoreboard(headers: Seq[GameHeader]): Unit = {
    //ToDo connect to MySQL and write the header data
  }

  private def checkStandingExist(date: String, headers: Seq[ConferenceStanding]): Boolean = {
    //ToDo check to see if any of these values have already been written to SQL, if so do not write
    //See if MySQL already does this for you... unique index on date and teamId?
    false
  }

  private def writeStandings(standings: Seq[ConferenceStanding]): Unit = {
    //ToDo connect to MySQL and write conference standing data
  }

}

private final class Arguments private(args: Array[String]) extends CommandLineBase(args, "Today's Games") {


  override protected def options: Options = super.options
    .addOption(Arguments.DateOption)

  def date: String = valueOf(Arguments.DateOption)
    .getOrElse(LocalDate.now().format(Arguments.NBADateFormat))

}

private object Arguments {

  private val NBADateFormat: DateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy")

  val DateOption: cli.Option = new cli.Option(null, "date", true, "The date you want to extract games from")

  def apply(args: Array[String]): Arguments = new Arguments(args)
}