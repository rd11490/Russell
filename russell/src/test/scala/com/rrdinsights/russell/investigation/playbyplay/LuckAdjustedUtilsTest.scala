package com.rrdinsights.russell.investigation.playbyplay

import java.{lang => jl}

import com.rrdinsights.russell.TestSpec
import com.rrdinsights.russell.investigation.playbyplay.luckadjusted.LuckAdjustedUtils
import com.rrdinsights.russell.storage.datamodel.{PlayByPlayEventMessageType, PlayByPlayWithLineup, ScoredShot}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class LuckAdjustedUtilsTest extends TestSpec {

  import LuckAdjustedUtilsTest._

  test("count possessions") {

    import PlayByPlayParserTestData._
    val sortedData = TestData
    val out = LuckAdjustedUtils.seperatePossessions(sortedData)

//    out.foreach(v => {
//      println("Possession:")
//      v._1.foreach(c => {
//        println(s"${c._1.timeElapsed} - ${c._1.eventNumber} - ${c._1.playType} - ${c._1.homeDescription} - ${c._1.neutralDescription} - ${c._1.awayDescription}")
//      })
//    })

    assert(out.size === 50)
  }

  test("count possessions2") {

    import PlayByPlayParserTestData2._
    val sortedData = TestData
    val out = LuckAdjustedUtils.seperatePossessions(sortedData)

    out.foreach(v => {
      println("Possession:")
      v._1.foreach(c => {
        println(s"${c._1.timeElapsed} - ${c._1.eventNumber} - ${c._1.playType} - ${c._1.homeDescription} - ${c._1.neutralDescription} - ${c._1.awayDescription}")
      })
    })

    assert(out.size === 49)
  }

  test("is And 1") {

    import PlayByPlayParserTestData._
    val sortedData = TestData.filter(_._1.timeElapsed == 454)
    val out = LuckAdjustedUtils.seperatePossessions(sortedData)

    assert(out.size === 1)
  }

  test("is And 1 - 2") {

    import PlayByPlayParserTestData2._
    val sortedData = TestData.filter(v => v._1.timeElapsed < 125)
    val out = LuckAdjustedUtils.seperatePossessions(sortedData)

//        out.foreach(v => {
//          println("Possession:")
//          v._1.foreach(c => {
//            println(s"${c._1.timeElapsed} - ${c._1.eventNumber} - ${c._1.playType} - ${c._1.homeDescription} - ${c._1.neutralDescription} - ${c._1.awayDescription}")
//          })
//        })

    assert(out.size === 7)
  }

  test("checkIfFTMade") {
    val event1 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.FreeThrow.toString, eventActionType = 15, homeDescription = "Shel. Williams Free Throw 3 of 3 (1 PTS)")
    val event2 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.FreeThrow.toString, eventActionType = 12, awayDescription = "Shel. Williams Free Throw 2 of 2 (1 PTS)")
    val event3 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.FreeThrow.toString, eventActionType = 10, homeDescription = "MISS Shel. Williams Free Throw 1 of 1")
    val event4 = buildPlayByPlayWithLineup(playType = PlayByPlayEventMessageType.FreeThrow.toString, eventActionType = 12, awayDescription = "MISS Shel. Williams Free Throw 2 of 2")

    assert(PlayByPlayUtils.isMadeFreeThrow((event1, None)))
    assert(PlayByPlayUtils.isMadeFreeThrow((event2, None)))
    assert(!PlayByPlayUtils.isMadeFreeThrow((event3, None)))
    assert(!PlayByPlayUtils.isMadeFreeThrow((event4, None)))
  }
}

private object LuckAdjustedUtilsTest {
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
                                 seasonType: String = null,
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
      seasonType,
      dt)
}