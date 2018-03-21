package com.rrdinsights.russell.investigation.playbyplay

import com.rrdinsights.russell.TestSpec
import com.rrdinsights.russell.storage.datamodel.{PlayByPlayEventMessageType, PlayByPlayWithLineup}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import java.{lang => jl}

@RunWith(classOf[JUnitRunner])
final class LuckAdjustedStintsTest extends TestSpec {

  import LuckAdjustedStintsTest._

  test("isEndOfPossession") {
    val event1 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.FreeThrow.toString, eventActionType = 14, homeDescription = "Shel. Williams Free Throw 2 of 3 (1 PTS)")
    val event2 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.FreeThrow.toString, eventActionType = 15, homeDescription = "Shel. Williams Free Throw 3 of 3 (1 PTS)")

    val event3 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.FreeThrow.toString, eventActionType = 14, homeDescription = "Shel. Williams Free Throw 2 of 3 (1 PTS)")
    val event4 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.FreeThrow.toString, player1TeamId = 1, eventActionType = 15, homeDescription = "MISS Shel. Williams Free Throw 3 of 3")
    val event5 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.Rebound.toString, player1TeamId = 2)

    val event6 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.Rebound.toString, player1TeamId = 1)

    val event7 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.Turnover.toString, player1TeamId = 2)

    val event8 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.Make.toString, player1TeamId = 2, timeElapsed = 4)

    assert(LuckAdjustedStints.isEndOfPossesion((event8, None), Seq((event1, None),(event2, None))))
    assert(!LuckAdjustedStints.isEndOfPossesion((event8, None), Seq((event3, None), (event4, None))))
    assert(LuckAdjustedStints.isEndOfPossesion((event8, None), Seq((event4, None),(event5, None))))
    assert(!LuckAdjustedStints.isEndOfPossesion((event8, None),Seq((event4, None),(event6, None))))
    assert(LuckAdjustedStints.isEndOfPossesion((event8, None), Seq((event6, None),(event7, None))))
    assert(LuckAdjustedStints.isEndOfPossesion((event8, None), Seq((event7, None))))
  }

  test("isEndOfPossession - substitution during FT") {
    val eventFT1 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.FreeThrow.toString, player1TeamId = 1, eventActionType = 13, homeDescription = "Shel. Williams Free Throw 1 of 3 (1 PTS)", timeElapsed = 1)
    val eventFT2 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.FreeThrow.toString, player1TeamId = 1, eventActionType = 14, homeDescription = "Shel. Williams Free Throw 2 of 3 (1 PTS)", timeElapsed = 1)
    val eventFT3 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.FreeThrow.toString, player1TeamId = 1, eventActionType = 15, homeDescription = "Shel. Williams Free Throw 3 of 3 (1 PTS)", timeElapsed = 1)
    val eventSub = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.Substitution.toString, player1TeamId = 2, timeElapsed = 1)
    val eventDRBD = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.Rebound.toString, player1TeamId = 2, timeElapsed = 2)
    val eventORBD = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.Rebound.toString, player1TeamId = 1, timeElapsed = 2)
    val eventDMake = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.Make.toString, player1TeamId = 2, timeElapsed = 2)
    val eventOMake = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.Make.toString, player1TeamId = 1, timeElapsed = 2)
    val eventEndOfPeriod = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.EndOfPeriod.toString, timeElapsed = 3)

    val eventsWithSubstitutionInFT3D = Seq(eventFT1, eventFT2, eventSub, eventFT3, eventDRBD, eventDMake, eventEndOfPeriod)
      .map(v => (v, None))

    val eventsWithSubstitutionInFT2D = Seq(eventFT1, eventSub, eventFT2, eventFT3, eventDRBD, eventDMake, eventEndOfPeriod)
      .map(v => (v, None))

    val eventsWithSubstitutionInFT3O = Seq(eventFT1, eventFT2, eventSub, eventFT3, eventORBD, eventOMake, eventEndOfPeriod)
      .map(v => (v, None))

    val eventsWithSubstitutionInFT2O = Seq(eventFT1, eventSub, eventFT2, eventFT3, eventORBD, eventOMake, eventEndOfPeriod)
      .map(v => (v, None))

    val seperatedStints1 = LuckAdjustedStints.seperatePossessions(eventsWithSubstitutionInFT3D).map(_._1)
    assert(seperatedStints1.size === 2)
    assert(seperatedStints1.head === eventsWithSubstitutionInFT3D.slice(0, 4))
    assert(seperatedStints1.last === eventsWithSubstitutionInFT3D.slice(4, 6))

    val seperatedStints2 = LuckAdjustedStints.seperatePossessions(eventsWithSubstitutionInFT2D).map(_._1)
    assert(seperatedStints2.size === 2)
    assert(seperatedStints2.head === eventsWithSubstitutionInFT2D.slice(0, 4))
    assert(seperatedStints2.last === eventsWithSubstitutionInFT2D.slice(4, 6))

    val seperatedStints3 = LuckAdjustedStints.seperatePossessions(eventsWithSubstitutionInFT3O).map(_._1)
    assert(seperatedStints3.size === 2)
    assert(seperatedStints3.head === eventsWithSubstitutionInFT3O.slice(0, 4))
    assert(seperatedStints3.last === eventsWithSubstitutionInFT3O.slice(4, 6))

    val seperatedStints4 = LuckAdjustedStints.seperatePossessions(eventsWithSubstitutionInFT2O).map(_._1)
    assert(seperatedStints4.size === 2)
    assert(seperatedStints4.head === eventsWithSubstitutionInFT2O.slice(0, 4))
    assert(seperatedStints4.last === eventsWithSubstitutionInFT2O.slice(4, 6))

  }
  
  test("checkIfFTMade") {
    val event1 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.FreeThrow.toString, eventActionType = 15, homeDescription = "Shel. Williams Free Throw 3 of 3 (1 PTS)")
    val event2 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.FreeThrow.toString, eventActionType = 12, awayDescription = "Shel. Williams Free Throw 2 of 2 (1 PTS)")
    val event3 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.FreeThrow.toString, eventActionType = 10, homeDescription = "MISS Shel. Williams Free Throw 1 of 1")
    val event4 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.FreeThrow.toString, eventActionType = 12, awayDescription = "MISS Shel. Williams Free Throw 2 of 2")

    assert(LuckAdjustedStints.checkIfFTMade((event1, None)))
    assert(LuckAdjustedStints.checkIfFTMade((event2, None)))
    assert(!LuckAdjustedStints.checkIfFTMade((event3, None)))
    assert(!LuckAdjustedStints.checkIfFTMade((event4, None)))
  }
}

private object LuckAdjustedStintsTest {
  def buildPlayByPlayWithLineup(
                                eventNumber: jl.Integer = 1,
                                playType: String = PlayByPlayEventMessageType.Make.toString,
                                eventActionType: jl.Integer = 0,
                                period: jl.Integer = 1,
                                wcTimeString: String = null,
                                pcTimeString: String = null,
                                homeDescription: String = null,
                                neutralDescription: String = null,
                                awayDescription: String = null,

                                homeScore: jl.Integer = null,
                                awayScore: jl.Integer = null,

                                player1Type: jl.Integer = null,
                                player1Id: jl.Integer = null,
                                player1TeamId: jl.Integer = null,

                                player2Type: jl.Integer = null,
                                player2Id: jl.Integer = null,
                                player2TeamId: jl.Integer = null,

                                player3Type: jl.Integer = null,
                                player3Id: jl.Integer = null,
                                player3TeamId: jl.Integer = null,

                                teamId1: jl.Integer = null,
                                team1player1Id: jl.Integer = null,
                                team1player2Id: jl.Integer = null,
                                team1player3Id: jl.Integer = null,
                                team1player4Id: jl.Integer = null,
                                team1player5Id: jl.Integer = null,
                                teamId2: jl.Integer = null,
                                team2player1Id: jl.Integer = null,
                                team2player2Id: jl.Integer = null,
                                team2player3Id: jl.Integer = null,
                                team2player4Id: jl.Integer = null,
                                team2player5Id: jl.Integer = null,

                                timeElapsed: jl.Integer = null,
                                season: String = null,
                                dt: String = null): PlayByPlayWithLineup =
    PlayByPlayWithLineup(
      null,
      null,
      eventNumber,
      playType,
      eventActionType,
      period,
      wcTimeString,
      pcTimeString,
      homeDescription,
      neutralDescription,
      awayDescription,

      homeScore,
      awayScore,

      player1Type,
      player1Id,
      player1TeamId,

      player2Type,
      player2Id,
      player2TeamId,

      player3Type,
      player3Id,
      player3TeamId,

      teamId1,
      team1player1Id,
      team1player2Id,
      team1player3Id,
      team1player4Id,
      team1player5Id,
      teamId2,
      team2player1Id,
      team2player2Id,
      team2player3Id,
      team2player4Id,
      team2player5Id,

      timeElapsed,
      season,
      dt)
}