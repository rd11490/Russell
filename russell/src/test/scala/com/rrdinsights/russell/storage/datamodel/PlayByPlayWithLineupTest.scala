package com.rrdinsights.russell.storage.datamodel

import com.rrdinsights.russell.TestSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class PlayByPlayWithLineupTest extends TestSpec {
  test("Sort PlayByPlayWithLineup by event number") {
    val time1 = buildPlayByPlayWithLineup(1, 100)
    val time2 = buildPlayByPlayWithLineup(2, 100)

    val times = Seq(time2, time1)

    val sortedTimes = times.sorted

    assert(sortedTimes.head == time1)
    assert(sortedTimes.last == time2)
  }

  test("Sort PlayByPlayWithLineup by time first") {
    val time1 = buildPlayByPlayWithLineup(1, 102)
    val time2 = buildPlayByPlayWithLineup(2, 100)

    val times = Seq(time2, time1)

    val sortedTimes = times.sorted

    assert(sortedTimes.head == time2)
    assert(sortedTimes.last == time1)
  }

  def buildPlayByPlayWithLineup(eventNum: Int, timeElapsed: Int): PlayByPlayWithLineup =
    PlayByPlayWithLineup(primaryKey = "0021700001_35", gameId = "0021700001", eventNumber = eventNum, playType = "Foul", eventActionType = 1, period = 1, wcTimeString = "8:08 PM", pcTimeString = "9:22", homeDescription = "Love P.FOUL (P1.T1) (M.McCutchen)", neutralDescription = "null", awayDescription = "null", homeScore = null, awayScore = null, player1Type = 4, player1Id = 201567, player1TeamId = 1610612739, player2Type = 5, player2Id = 202681, player2TeamId = 1610612738, player3Type = 1, player3Id = 0, player3TeamId = null, teamId1 = 1610612738, team1player1Id = 201143, team1player2Id = 202330, team1player3Id = 202681, team1player4Id = 1627759, team1player5Id = 1628369, teamId2 = 1610612739, team2player1Id = 2544, team2player2Id = 2548, team2player3Id = 201565, team2player4Id = 201567, team2player5Id = 203109, timeElapsed = timeElapsed, season = "2017-18", dt = "2018-04-14T11:09:43.165")

}
