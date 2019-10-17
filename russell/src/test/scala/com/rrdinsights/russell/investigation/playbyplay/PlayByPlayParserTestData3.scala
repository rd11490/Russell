package com.rrdinsights.russell.investigation.playbyplay

import com.rrdinsights.russell.storage.datamodel.{PlayByPlayWithLineup, ScoredShot}

object PlayByPlayParserTestData3 {

  val TestData: Seq[(PlayByPlayWithLineup, Option[ScoredShot])] = Seq(
    (PlayByPlayWithLineup(primaryKey = "0021700001_35", gameId = "0021700001", eventNumber = 1, playType = "JumpBall", eventActionType = 1, period = 1, wcTimeString = "8:08 PM", pcTimeString = "12:00", homeDescription = "Love P.FOUL (P1.T1) (M.McCutchen)", neutralDescription = "null", awayDescription = "null", homeScore = null, awayScore = null, player1Type = 4, player1Id = 201567, player1TeamId = 1610612739, player2Type = 5, player2Id = 202681, player2TeamId = 1610612738, player3Type = 1, player3Id = 0, player3TeamId = null, teamId1 = 1610612738, team1player1Id = 201143, team1player2Id = 202330, team1player3Id = 202681, team1player4Id = 1627759, team1player5Id = 1628369, teamId2 = 1610612739, team2player1Id = 2544, team2player2Id = 2548, team2player3Id = 201565, team2player4Id = 201567, team2player5Id = 203109, timeElapsed = 0, season = "2017-18", seasonType = "Regular Season", dt = "2018-04-14T11:09:43.165"), None),
    (PlayByPlayWithLineup(primaryKey = "0021700001_50", gameId = "0021700001", eventNumber = 2, playType = "Rebound", eventActionType = 0, period = 1, wcTimeString = "8:11 PM", pcTimeString = "11:34", homeDescription = "null", neutralDescription = "null", awayDescription = "Irving 16 Step Back Jump Shot (4 PTS)", homeScore = 8, awayScore = 9, player1Type = 5, player1Id = 202681, player1TeamId = 1610612738, player2Type = 0, player2Id = 0, player2TeamId = null, player3Type = 0, player3Id = 0, player3TeamId = null, teamId1 = 1610612738, team1player1Id = 201143, team1player2Id = 202330, team1player3Id = 202681, team1player4Id = 1627759, team1player5Id = 1628369, teamId2 = 1610612739, team2player1Id = 2544, team2player2Id = 2548, team2player3Id = 201565, team2player4Id = 201567, team2player5Id = 203109, timeElapsed = 26, season = "2017-18", seasonType = "Regular Season", dt = "2018-04-14T11:09:43.165"), Some(ScoredShot(primaryKey = "0021700001_50", gameId = "0021700001", eventNumber = 50, shooter = 202681, offenseTeamId = 1610612738, offensePlayer1Id = 201143, offensePlayer2Id = 202330, offensePlayer3Id = 202681, offensePlayer4Id = 1627759, offensePlayer5Id = 1628369, defenseTeamId = 1610612739, defensePlayer1Id = 2544, defensePlayer2Id = 2548, defensePlayer3Id = 201565, defensePlayer4Id = 201567, defensePlayer5Id = 203109, bin = "Right18FT", shotValue = 2, shotAttempted = 1, shotMade = 1, expectedPoints = 0.9695885509838998, playerShotAttempted = 559, playerShotMade = 271, season = "2017-18", seasonType = "Regular Season", dt = "2018-04-14T11:01:00.502"))),
    (PlayByPlayWithLineup(primaryKey = "0021700001_142", gameId = "0021700001", eventNumber = 3, playType = "Miss", eventActionType = 1, period = 1, wcTimeString = "8:32 PM", pcTimeString = "11:34", homeDescription = "Smith 9 Jump Shot (5 PTS)", neutralDescription = "null", awayDescription = "null", homeScore = 17, awayScore = 25, player1Type = 4, player1Id = 2747, player1TeamId = 1610612739, player2Type = 0, player2Id = 0, player2TeamId = null, player3Type = 0, player3Id = 0, player3TeamId = null, teamId1 = 1610612738, team1player1Id = 203382, team1player2Id = 203935, team1player3Id = 1626179, team1player4Id = 1627759, team1player5Id = 1628400, teamId2 = 1610612739, team2player1Id = 2548, team2player2Id = 2747, team2player3Id = 201145, team2player4Id = 202684, team2player5Id = 202697, timeElapsed = 26, season = "2017-18", seasonType = "Regular Season", dt = "2018-04-14T11:09:43.165"), Some(ScoredShot(primaryKey = "0021700001_142", gameId = "0021700001", eventNumber = 142, shooter = 2747, offenseTeamId = 1610612739, offensePlayer1Id = 2548, offensePlayer2Id = 2747, offensePlayer3Id = 201145, offensePlayer4Id = 202684, offensePlayer5Id = 202697, defenseTeamId = 1610612738, defensePlayer1Id = 203382, defensePlayer2Id = 203935, defensePlayer3Id = 1626179, defensePlayer4Id = 1627759, defensePlayer5Id = 1628400, bin = "Right11FT", shotValue = 2, shotAttempted = 1, shotMade = 1, expectedPoints = 0.755700325732899, playerShotAttempted = 307, playerShotMade = 116, season = "2017-18", seasonType = "Regular Season", dt = "2018-04-14T11:01:00.502"))),
    )
}
