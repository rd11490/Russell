package com.rrdinsights.russell.investigation

import java.{lang => jl}

import com.rrdinsights.russell.TestSpec
import com.rrdinsights.russell.storage.datamodel.{PlayByPlayEventMessageType, PlayersOnCourt, RawPlayByPlayEvent}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class PlayByPlayParserTest extends TestSpec {
  test("sort Play by Play") {
    val playByPlay = Seq(
      buildRawPlayByPlayEvent(1, PlayByPlayEventMessageType.StartOfPeriod),
      buildRawPlayByPlayEvent(2, PlayByPlayEventMessageType.Miss),
      buildRawPlayByPlayEvent(3, PlayByPlayEventMessageType.Foul),
      buildRawPlayByPlayEvent(4, PlayByPlayEventMessageType.FreeThrow),
      buildRawPlayByPlayEvent(5, PlayByPlayEventMessageType.Substitution),
      buildRawPlayByPlayEvent(6, PlayByPlayEventMessageType.FreeThrow))

    val sortedPlayByPlay = PlayByPlayParser.properlySortPlayByPlay(playByPlay)

    assert(sortedPlayByPlay.map(v => PlayByPlayEventMessageType.valueOf(v.playType)) === Seq(
      PlayByPlayEventMessageType.StartOfPeriod,
      PlayByPlayEventMessageType.Miss,
      PlayByPlayEventMessageType.Foul,
      PlayByPlayEventMessageType.FreeThrow,
      PlayByPlayEventMessageType.FreeThrow,
      PlayByPlayEventMessageType.Substitution))
  }

  test("sort Play by Play2") {
    val playByPlay = Seq(
      buildRawPlayByPlayEvent(1, PlayByPlayEventMessageType.StartOfPeriod),
      buildRawPlayByPlayEvent(2, PlayByPlayEventMessageType.Miss),
      buildRawPlayByPlayEvent(3, PlayByPlayEventMessageType.Foul),
      buildRawPlayByPlayEvent(4, PlayByPlayEventMessageType.FreeThrow),
      buildRawPlayByPlayEvent(5, PlayByPlayEventMessageType.Substitution),
      buildRawPlayByPlayEvent(6, PlayByPlayEventMessageType.FreeThrow),
      buildRawPlayByPlayEvent(7, PlayByPlayEventMessageType.FreeThrow))

    val sortedPlayByPlay = PlayByPlayParser.properlySortPlayByPlay(playByPlay)

    assert(sortedPlayByPlay.map(v => PlayByPlayEventMessageType.valueOf(v.playType)) === Seq(
      PlayByPlayEventMessageType.StartOfPeriod,
      PlayByPlayEventMessageType.Miss,
      PlayByPlayEventMessageType.Foul,
      PlayByPlayEventMessageType.FreeThrow,
      PlayByPlayEventMessageType.FreeThrow,
      PlayByPlayEventMessageType.FreeThrow,
      PlayByPlayEventMessageType.Substitution))
  }
  test("sort Play by Play3") {
    val playByPlay = Seq(
      buildRawPlayByPlayEvent(1, PlayByPlayEventMessageType.StartOfPeriod),
      buildRawPlayByPlayEvent(2, PlayByPlayEventMessageType.Miss),
      buildRawPlayByPlayEvent(3, PlayByPlayEventMessageType.Substitution),
      buildRawPlayByPlayEvent(4, PlayByPlayEventMessageType.Foul),
      buildRawPlayByPlayEvent(5, PlayByPlayEventMessageType.FreeThrow),
      buildRawPlayByPlayEvent(6, PlayByPlayEventMessageType.FreeThrow))

    val sortedPlayByPlay = PlayByPlayParser.properlySortPlayByPlay(playByPlay)

    assert(sortedPlayByPlay.map(v => PlayByPlayEventMessageType.valueOf(v.playType)) === Seq(
      PlayByPlayEventMessageType.StartOfPeriod,
      PlayByPlayEventMessageType.Miss,
      PlayByPlayEventMessageType.Substitution,
      PlayByPlayEventMessageType.Foul,
      PlayByPlayEventMessageType.FreeThrow,
      PlayByPlayEventMessageType.FreeThrow))
  }

  test("Test Substitutions") {
    val teamId1 = 1

    val player1Team1 = 1
    val player2Team1 = 2
    val player3Team1 = 3
    val player4Team1 = 4
    val player5Team1 = 5
    val player6Team1 = 6
    val player7Team1 = 7

    val teamId2 = 2

    val player1Team2 = 11
    val player2Team2 = 12
    val player3Team2 = 13
    val player4Team2 = 14
    val player5Team2 = 15
    val player6Team2 = 16
    val player7Team2 = 17

    val starters = Seq(
      PlayersOnCourt("1", "1",1, teamId1, player1Team1, player2Team1, player3Team1, player4Team1, player5Team1,
        teamId2, player1Team2, player2Team2, player3Team2, player4Team2, player5Team2, "", ""),
      PlayersOnCourt("1", "1",2, teamId1, player1Team1, player2Team1, player3Team1, player4Team1, player5Team1,
        teamId2, player1Team2, player2Team2, player3Team2, player4Team2, player5Team2, "", ""))


    val playByPlay = Seq(
      buildRawPlayByPlayEvent(1, PlayByPlayEventMessageType.StartOfPeriod, period = 1),
      buildRawPlayByPlayEvent(2, PlayByPlayEventMessageType.Miss),
      buildRawPlayByPlayEvent(3, PlayByPlayEventMessageType.Substitution, player1TeamId = 1),
      buildRawPlayByPlayEvent(4, PlayByPlayEventMessageType.Foul),
      buildRawPlayByPlayEvent(5, PlayByPlayEventMessageType.FreeThrow),
      buildRawPlayByPlayEvent(6, PlayByPlayEventMessageType.FreeThrow),
      buildRawPlayByPlayEvent(2, PlayByPlayEventMessageType.Miss),
      buildRawPlayByPlayEvent(3, PlayByPlayEventMessageType.Substitution),
      buildRawPlayByPlayEvent(4, PlayByPlayEventMessageType.Foul),
      buildRawPlayByPlayEvent(5, PlayByPlayEventMessageType.FreeThrow),
      buildRawPlayByPlayEvent(6, PlayByPlayEventMessageType.FreeThrow),
      buildRawPlayByPlayEvent(2, PlayByPlayEventMessageType.Miss),
      buildRawPlayByPlayEvent(3, PlayByPlayEventMessageType.Substitution),
      buildRawPlayByPlayEvent(4, PlayByPlayEventMessageType.Foul),
      buildRawPlayByPlayEvent(5, PlayByPlayEventMessageType.FreeThrow),
      buildRawPlayByPlayEvent(6, PlayByPlayEventMessageType.FreeThrow),
      buildRawPlayByPlayEvent(2, PlayByPlayEventMessageType.Miss),
      buildRawPlayByPlayEvent(3, PlayByPlayEventMessageType.Substitution),
      buildRawPlayByPlayEvent(4, PlayByPlayEventMessageType.Foul),
      buildRawPlayByPlayEvent(5, PlayByPlayEventMessageType.FreeThrow),
      buildRawPlayByPlayEvent(6, PlayByPlayEventMessageType.FreeThrow),
      buildRawPlayByPlayEvent(1, PlayByPlayEventMessageType.StartOfPeriod, period = 2),
      buildRawPlayByPlayEvent(2, PlayByPlayEventMessageType.Miss),
      buildRawPlayByPlayEvent(3, PlayByPlayEventMessageType.Substitution))
  }

  private def buildRawPlayByPlayEvent(eventNumber: jl.Integer,
                                      playType: PlayByPlayEventMessageType,
                                      period: jl.Integer = null,
                                      player1Id: jl.Integer = null,
                                      player1TeamId: jl.Integer = null,
                                      player2Id: jl.Integer = null,
                                      player2TeamId: jl.Integer = null): RawPlayByPlayEvent =
    RawPlayByPlayEvent(
      s"1_$eventNumber",
      "1",
      eventNumber,
      playType.toString,
      null,
      null,
      null,
      null,
      null,
      null,
      null,

      null,
      null,

      null,
      player1Id,
      null,
      player1TeamId,
      null,
      null,
      null,

      null,
      player2Id,
      null,
      player2TeamId,
      null,
      null,
      null,

      null,
      null,
      null,
      null,
      null,
      null,
      null,

      null,
      null)
}
