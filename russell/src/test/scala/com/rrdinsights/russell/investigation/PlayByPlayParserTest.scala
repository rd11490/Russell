package com.rrdinsights.russell.investigation

import java.{lang => jl}

import com.rrdinsights.russell.TestSpec
import com.rrdinsights.russell.investigation.playbyplay.PlayByPlayParser
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
      buildPlayersOnCourt(null, 1, teamId1, player1Team1, player2Team1, player3Team1, player4Team1, player5Team1,
        teamId2, player1Team2, player2Team2, player3Team2, player4Team2, player5Team2),
      buildPlayersOnCourt(null, 2, teamId1, player1Team1, player2Team1, player3Team1, player4Team1, player5Team1,
        teamId2, player1Team2, player2Team2, player3Team2, player4Team2, player5Team2))

    val playByPlay = Seq(
      buildRawPlayByPlayEvent(1, PlayByPlayEventMessageType.StartOfPeriod, period = 1),
      buildRawPlayByPlayEvent(2, PlayByPlayEventMessageType.Miss, period = 1),
      buildRawPlayByPlayEvent(3, PlayByPlayEventMessageType.Make, period = 1),
      buildRawPlayByPlayEvent(4, PlayByPlayEventMessageType.Foul, period = 1),
      buildRawPlayByPlayEvent(5, PlayByPlayEventMessageType.FreeThrow, period = 1),
      buildRawPlayByPlayEvent(6, PlayByPlayEventMessageType.FreeThrow, period = 1),
      buildRawPlayByPlayEvent(7, PlayByPlayEventMessageType.Miss, period = 1),
      buildRawPlayByPlayEvent(8, PlayByPlayEventMessageType.Substitution, period = 1, player1TeamId = 1, player2TeamId = 1, player1Id = player1Team1, player2Id = player6Team1),
      buildRawPlayByPlayEvent(9, PlayByPlayEventMessageType.Substitution, period = 1, player1TeamId = 2, player2TeamId =2, player1Id = player1Team2, player2Id = player6Team2),
      buildRawPlayByPlayEvent(10, PlayByPlayEventMessageType.Foul, period = 1),
      buildRawPlayByPlayEvent(11, PlayByPlayEventMessageType.FreeThrow, period = 1),
      buildRawPlayByPlayEvent(12, PlayByPlayEventMessageType.FreeThrow, period = 1),
      buildRawPlayByPlayEvent(13, PlayByPlayEventMessageType.Miss, period = 1),
      buildRawPlayByPlayEvent(14, PlayByPlayEventMessageType.Substitution, period = 1, player1TeamId = 2, player2TeamId =2, player1Id = player4Team2, player2Id = player7Team2),
      buildRawPlayByPlayEvent(15, PlayByPlayEventMessageType.Foul, period = 1),
      buildRawPlayByPlayEvent(16, PlayByPlayEventMessageType.FreeThrow, period = 1),
      buildRawPlayByPlayEvent(17, PlayByPlayEventMessageType.FreeThrow, period = 1),
      buildRawPlayByPlayEvent(18, PlayByPlayEventMessageType.Miss, period = 1),
      buildRawPlayByPlayEvent(19, PlayByPlayEventMessageType.Substitution, period = 1, player1TeamId = 1, player2TeamId =1, player1Id = player3Team1, player2Id = player7Team1),
      buildRawPlayByPlayEvent(20, PlayByPlayEventMessageType.Foul, period = 1),
      buildRawPlayByPlayEvent(21, PlayByPlayEventMessageType.FreeThrow, period = 1),
      buildRawPlayByPlayEvent(22, PlayByPlayEventMessageType.FreeThrow, period = 1),
      buildRawPlayByPlayEvent(23, PlayByPlayEventMessageType.StartOfPeriod, period = 2),
      buildRawPlayByPlayEvent(24, PlayByPlayEventMessageType.Miss, period = 2),
      buildRawPlayByPlayEvent(25, PlayByPlayEventMessageType.Substitution, period = 2, player1TeamId = 1, player2TeamId =1, player1Id = player5Team1, player2Id = player6Team1))

    val parser = new PlayByPlayParser(playByPlay, starters, null)

    val playersOnCourt = parser.run()

    assert(playersOnCourt === Seq(
      buildPlayersOnCourt(1, 1, teamId1, player1Team1, player2Team1, player3Team1, player4Team1, player5Team1,
        teamId2, player1Team2, player2Team2, player3Team2, player4Team2, player5Team2),
      buildPlayersOnCourt(2, 1, teamId1, player1Team1, player2Team1, player3Team1, player4Team1, player5Team1,
        teamId2, player1Team2, player2Team2, player3Team2, player4Team2, player5Team2),
      buildPlayersOnCourt(3, 1, teamId1, player1Team1, player2Team1, player3Team1, player4Team1, player5Team1,
        teamId2, player1Team2, player2Team2, player3Team2, player4Team2, player5Team2),
      buildPlayersOnCourt(4, 1, teamId1, player1Team1, player2Team1, player3Team1, player4Team1, player5Team1,
        teamId2, player1Team2, player2Team2, player3Team2, player4Team2, player5Team2),
      buildPlayersOnCourt(5, 1, teamId1, player1Team1, player2Team1, player3Team1, player4Team1, player5Team1,
        teamId2, player1Team2, player2Team2, player3Team2, player4Team2, player5Team2),
      buildPlayersOnCourt(6, 1, teamId1, player1Team1, player2Team1, player3Team1, player4Team1, player5Team1,
        teamId2, player1Team2, player2Team2, player3Team2, player4Team2, player5Team2),
      buildPlayersOnCourt(7, 1, teamId1, player1Team1, player2Team1, player3Team1, player4Team1, player5Team1,
        teamId2, player1Team2, player2Team2, player3Team2, player4Team2, player5Team2),
      buildPlayersOnCourt(8, 1, teamId1, player2Team1, player3Team1, player4Team1, player5Team1, player6Team1,
        teamId2, player1Team2, player2Team2, player3Team2, player4Team2, player5Team2),
      buildPlayersOnCourt(9, 1, teamId1, player2Team1, player3Team1, player4Team1, player5Team1, player6Team1,
        teamId2, player2Team2, player3Team2, player4Team2, player5Team2, player6Team2),
      buildPlayersOnCourt(10, 1, teamId1, player2Team1, player3Team1, player4Team1, player5Team1, player6Team1,
        teamId2, player2Team2, player3Team2, player4Team2, player5Team2, player6Team2),
      buildPlayersOnCourt(11, 1, teamId1, player2Team1, player3Team1, player4Team1, player5Team1, player6Team1,
        teamId2, player2Team2, player3Team2, player4Team2, player5Team2, player6Team2),
      buildPlayersOnCourt(12, 1, teamId1, player2Team1, player3Team1, player4Team1, player5Team1, player6Team1,
        teamId2, player2Team2, player3Team2, player4Team2, player5Team2, player6Team2),
      buildPlayersOnCourt(13, 1, teamId1, player2Team1, player3Team1, player4Team1, player5Team1, player6Team1,
        teamId2, player2Team2, player3Team2, player4Team2, player5Team2, player6Team2),
      buildPlayersOnCourt(14, 1, teamId1, player2Team1, player3Team1, player4Team1, player5Team1, player6Team1,
        teamId2, player2Team2, player3Team2, player5Team2, player6Team2, player7Team2),
      buildPlayersOnCourt(15, 1, teamId1, player2Team1, player3Team1, player4Team1, player5Team1, player6Team1,
        teamId2, player2Team2, player3Team2, player5Team2, player6Team2, player7Team2),
      buildPlayersOnCourt(16, 1, teamId1, player2Team1, player3Team1, player4Team1, player5Team1, player6Team1,
        teamId2, player2Team2, player3Team2, player5Team2, player6Team2, player7Team2),
      buildPlayersOnCourt(17, 1, teamId1, player2Team1, player3Team1, player4Team1, player5Team1, player6Team1,
        teamId2, player2Team2, player3Team2, player5Team2, player6Team2, player7Team2),
      buildPlayersOnCourt(18, 1, teamId1, player2Team1, player3Team1, player4Team1, player5Team1, player6Team1,
        teamId2, player2Team2, player3Team2, player5Team2, player6Team2, player7Team2),
      buildPlayersOnCourt(19, 1, teamId1, player2Team1, player4Team1, player5Team1, player6Team1, player7Team1,
        teamId2, player2Team2, player3Team2, player5Team2, player6Team2, player7Team2),
      buildPlayersOnCourt(20, 1, teamId1, player2Team1, player4Team1, player5Team1, player6Team1, player7Team1,
        teamId2, player2Team2, player3Team2, player5Team2, player6Team2, player7Team2),
      buildPlayersOnCourt(21, 1, teamId1, player2Team1, player4Team1, player5Team1, player6Team1, player7Team1,
        teamId2, player2Team2, player3Team2, player5Team2, player6Team2, player7Team2),
      buildPlayersOnCourt(22, 1, teamId1, player2Team1, player4Team1, player5Team1, player6Team1, player7Team1,
        teamId2, player2Team2, player3Team2, player5Team2, player6Team2, player7Team2),
      buildPlayersOnCourt(23, 2, teamId1, player1Team1, player2Team1, player3Team1, player4Team1, player5Team1,
        teamId2, player1Team2, player2Team2, player3Team2, player4Team2, player5Team2),
      buildPlayersOnCourt(24, 2, teamId1, player1Team1, player2Team1, player3Team1, player4Team1, player5Team1,
        teamId2, player1Team2, player2Team2, player3Team2, player4Team2, player5Team2),
      buildPlayersOnCourt(25, 2, teamId1, player1Team1, player2Team1, player3Team1, player4Team1, player6Team1,
        teamId2, player1Team2, player2Team2, player3Team2, player4Team2, player5Team2)
    ))
  }

  private def buildPlayersOnCourt(eventNumber: jl.Integer,
                                  period: jl.Integer,
                                  teamId1: jl.Integer,
                                  team1Player1: jl.Integer,
                                  team1Player2: jl.Integer,
                                  team1Player3: jl.Integer,
                                  team1Player4: jl.Integer,
                                  team1Player5: jl.Integer,
                                  teamId2: jl.Integer,
                                  team2Player1: jl.Integer,
                                  team2Player2: jl.Integer,
                                  team2Player3: jl.Integer,
                                  team2Player4: jl.Integer,
                                  team2Player5: jl.Integer): PlayersOnCourt =
    PlayersOnCourt(
      s"1_$eventNumber",
      "1",
      eventNumber,
      period,
      teamId1,
      team1Player1,
      team1Player2,
      team1Player3,
      team1Player4,
      team1Player5,
      teamId2,
      team2Player1,
      team2Player2,
      team2Player3,
      team2Player4,
      team2Player5,
      null,
      null)

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
      period,
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
