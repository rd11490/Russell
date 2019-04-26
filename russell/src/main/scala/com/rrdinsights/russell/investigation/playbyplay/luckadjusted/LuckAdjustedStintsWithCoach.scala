//package com.rrdinsights.russell.investigation.playbyplay.luckadjusted
//
//import java.{lang => jl}
//
//import com.rrdinsights.russell.investigation.shots.ShotUtils
//import com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedPointsArguments
//import com.rrdinsights.russell.storage.MySqlClient
//import com.rrdinsights.russell.storage.datamodel._
//import com.rrdinsights.russell.storage.tables.NBATables
//import com.rrdinsights.russell.utils.{MapJoin, TimeUtils}
//
//object LuckAdjustedStintsWithCoach {
//
//  /**
//    * This object is for calculating stints for luck adjusted RAPM
//    *
//    */
//  def main(strings: Array[String]): Unit = {
//    val dt = TimeUtils.dtNow
//    val args = ExpectedPointsArguments(strings)
//    val season = args.season
//    val seasonType = args.seasonType
//    val whereSeason = s"season = '$season'"
//    val whereSeasonType = s"seasontype = '$seasonType'"
//    val playByPlay = LuckAdjustedUtils.readPlayByPlay(whereSeason, whereSeasonType)
//
//    val freeThrowMap = LuckAdjustedUtils.buildPlayerCareerFreeThrowPercentMap()
//
//    val seasonShots = ShotUtils.readScoredShots(whereSeason)
//
//    val keyedPbP = playByPlay.map(v => ((v.gameId, v.eventNumber), v)).toMap
//    val keyedShots = seasonShots.map(v => ((v.gameId, v.eventNumber), v)).toMap
//
//    val playByPlayWithShotData = MapJoin.leftOuterJoin(keyedPbP, keyedShots)
//
//    val coaches = coachMap()
//
//    val possessions = playByPlayWithShotData
//      .groupBy(v => (v._1.gameId, v._1.period))
//      .flatMap(v => {
//        val sorted = v._2.sortBy(_._1)
//        LuckAdjustedUtils
//          .seperatePossessions(sorted)
//          .map(c => parsePossesions(c, sorted, freeThrowMap, dt))
//      })
//        .toSeq
//
//
//    val stints = possessions.groupBy(_.primaryKey)
//      .map(v => v._2.reduce(_ + _))
//      .toSeq
//
//
//    writeStints(stints)
//
//    val oneWayStints = stints
//      .flatMap(_.toOneWayStints)
//      .groupBy(_.primaryKey)
//      .map(v => v._2.reduce(_ + _))
//      .toSeq
//
//    writeOneWayStints(oneWayStints)
//  }
//
//  private def coachMap(/*IO*/): Map[jl.Integer, RosterCoach] = {
//    MySqlClient.selectFrom()
//  }
//
//  private def writeStints(stints: Seq[LuckAdjustedStintWithCoach]): Unit = {
//    MySqlClient.createTable(NBATables.luck_adjusted_stints_with_coach)
//    MySqlClient.insertInto(NBATables.luck_adjusted_stints_with_coach, stints)
//  }
//
//  private def writeOneWayStints(stints: Seq[LuckAdjustedOneWayStintWithCoach]): Unit = {
//    MySqlClient.createTable(NBATables.luck_adjusted_one_way_stints_with_coach)
//    MySqlClient.insertInto(NBATables.luck_adjusted_one_way_stints_with_coach, stints)
//  }
//
//  private def parsePossesions(eventsWithTime: (Seq[(PlayByPlayWithLineup,
//    Option[ScoredShot])],
//    Integer),
//                              allEvents: Seq[(PlayByPlayWithLineup,
//                                Option[ScoredShot])],
//                              freeThrowMap: Map[jl.Integer, jl.Double],
//                              coachMap: Map[jl.Integer, RosterCoach],
//                              dt: String): LuckAdjustedStintWithCoach = {
//
//    val events = eventsWithTime._1
//    val time = eventsWithTime._2
//    val points = LuckAdjustedUtils.countPoints(events, freeThrowMap)
//    val turnovers = LuckAdjustedUtils.countTurnovers(events)
//    val rebounds = LuckAdjustedUtils.countRebounds(events, allEvents)
//
//    val first = events.head
//    val team1Points = points.find(_.teamId == first._1.teamId1)
//    val team2Points = points.find(_.teamId == first._1.teamId2)
//    val team1Turnovers =
//      turnovers.find(_.teamId == first._1.teamId1).map(_.turnovers)
//    val team2Turnovers =
//      turnovers.find(_.teamId == first._1.teamId2).map(_.turnovers)
//    val team1Rebounds = rebounds.find(_.teamId == first._1.teamId1)
//    val team2Rebounds = rebounds.find(_.teamId == first._1.teamId2)
//
//    val primaryKey = Seq(
//      first._1.team1player1Id,
//      first._1.team1player2Id,
//      first._1.team1player3Id,
//      first._1.team1player4Id,
//      first._1.team1player5Id,
//      first._1.team2player1Id,
//      first._1.team2player2Id,
//      first._1.team2player3Id,
//      first._1.team2player4Id,
//      first._1.team2player5Id,
//      first._1.season
//    ).mkString("_")
//
//    val possession = LuckAdjustedUtils.determinePossession(events)
//    val team1Possessions = if (possession == first._1.teamId1) 1 else 0
//    val team2Possessions = if (possession == first._1.teamId2) 1 else 0
//
//    LuckAdjustedStintWithCoach(
//      primaryKey = primaryKey,
//      season = first._1.season,
//      seasonType = first._1.seasonType,
//      dt = dt,
//      teamId1 = first._1.teamId1,
//      team1player1Id = first._1.team1player1Id,
//      team1player2Id = first._1.team1player2Id,
//      team1player3Id = first._1.team1player3Id,
//      team1player4Id = first._1.team1player4Id,
//      team1player5Id = first._1.team1player5Id,
//      teamId2 = first._1.teamId2,
//      team2player1Id = first._1.team2player1Id,
//      team2player2Id = first._1.team2player2Id,
//      team2player3Id = first._1.team2player3Id,
//      team2player4Id = first._1.team2player4Id,
//      team2player5Id = first._1.team2player5Id,
//      team1Points = team1Points.map(_.points).getOrElse(0.0),
//      team1ExpectedPoints = team1Points.map(_.expectedPoints).getOrElse(0.0),
//      team2Points = team2Points.map(_.points).getOrElse(0.0),
//      team2ExpectedPoints = team2Points.map(_.expectedPoints).getOrElse(0.0),
//      team1DefensiveRebounds =
//        team1Rebounds.map(_.defensiveRebounds).getOrElse(0),
//      team2DefensiveRebounds =
//        team2Rebounds.map(_.defensiveRebounds).getOrElse(0),
//      team1OffensiveRebounds =
//        team1Rebounds.map(_.offensiveRebounds).getOrElse(0),
//      team2OffensiveRebounds =
//        team2Rebounds.map(_.offensiveRebounds).getOrElse(0),
//      team1Turnovers = team1Turnovers.getOrElse(0),
//      team2Turnovers = team2Turnovers.getOrElse(0),
//
//      team1FieldGoalAttempts = team1Points.map(_.fieldGoalAttempts).getOrElse(0),
//      team1FieldGoals = team1Points.map(_.fieldGoals).getOrElse(0),
//      team1ThreePtAttempts = team1Points.map(_.threePtAttempts).getOrElse(0),
//      team1ThreePtMade = team1Points.map(_.threePtMade).getOrElse(0),
//      team1FreeThrowAttempts = team1Points.map(_.freeThrowAttempts).getOrElse(0),
//      team1FreeThrowsMade = team1Points.map(_.freeThrowsMade).getOrElse(0),
//
//      team2FieldGoalAttempts = team2Points.map(_.fieldGoalAttempts).getOrElse(0),
//      team2FieldGoals = team2Points.map(_.fieldGoals).getOrElse(0),
//      team2ThreePtAttempts = team2Points.map(_.threePtAttempts).getOrElse(0),
//      team2ThreePtMade = team2Points.map(_.threePtMade).getOrElse(0),
//      team2FreeThrowAttempts = team2Points.map(_.freeThrowAttempts).getOrElse(0),
//      team2FreeThrowsMade = team2Points.map(_.freeThrowsMade).getOrElse(0),
//
//      team1Possessions = team1Possessions,
//      team2Possessions = team2Possessions,
//      seconds = time
//    )
//  }
//}
//
//final case class LuckAdjustedStintWithCoach(primaryKey: String,
//                                   season: String,
//                                   seasonType: String,
//                                   dt: String,
//                                   teamId1: jl.Integer,
//                                   team1player1Id: jl.Integer,
//                                   team1player2Id: jl.Integer,
//                                   team1player3Id: jl.Integer,
//                                   team1player4Id: jl.Integer,
//                                   team1player5Id: jl.Integer,
//                                   teamId2: jl.Integer,
//                                   team2player1Id: jl.Integer,
//                                   team2player2Id: jl.Integer,
//                                   team2player3Id: jl.Integer,
//                                   team2player4Id: jl.Integer,
//                                   team2player5Id: jl.Integer,
//                                   team1Points: jl.Double = 0.0,
//                                   team1ExpectedPoints: jl.Double = 0.0,
//                                   team2Points: jl.Double = 0.0,
//                                   team2ExpectedPoints: jl.Double = 0.0,
//                                   team1Assists: jl.Integer = 0,
//                                   team2Assists: jl.Integer = 0,
//                                   team1DefensiveRebounds: jl.Integer = 0,
//                                   team2DefensiveRebounds: jl.Integer = 0,
//                                   team1OffensiveRebounds: jl.Integer = 0,
//                                   team2OffensiveRebounds: jl.Integer = 0,
//                                   team1Fouls: jl.Integer = 0,
//                                   team2Fouls: jl.Integer = 0,
//                                   team1Turnovers: jl.Integer = 0,
//                                   team2Turnovers: jl.Integer = 0,
//                                   team1Possessions: jl.Integer = 0,
//                                   team2Possessions: jl.Integer = 0,
//                                   team1FieldGoalAttempts: jl.Integer = 0,
//                                   team1FieldGoals: jl.Integer = 0,
//                                   team1ThreePtAttempts: jl.Integer = 0,
//                                   team1ThreePtMade: jl.Integer = 0,
//                                   team1FreeThrowAttempts: jl.Integer = 0,
//                                   team1FreeThrowsMade: jl.Integer = 0,
//                                   team2FieldGoalAttempts: jl.Integer = 0,
//                                   team2FieldGoals: jl.Integer = 0,
//                                   team2ThreePtAttempts: jl.Integer = 0,
//                                   team2ThreePtMade: jl.Integer = 0,
//                                   team2FreeThrowAttempts: jl.Integer = 0,
//                                   team2FreeThrowsMade: jl.Integer = 0,
//                                   seconds: jl.Integer = 0) {
//  def +(other: LuckAdjustedStintWithCoach): LuckAdjustedStintWithCoach =
//    LuckAdjustedStintWithCoach(
//      primaryKey = primaryKey,
//      season = season,
//      seasonType = seasonType,
//      dt = dt,
//      teamId1 = teamId1,
//      team1player1Id = team1player1Id,
//      team1player2Id = team1player2Id,
//      team1player3Id = team1player3Id,
//      team1player4Id = team1player4Id,
//      team1player5Id = team1player5Id,
//      teamId2 = teamId2,
//      team2player1Id = team2player1Id,
//      team2player2Id = team2player2Id,
//      team2player3Id = team2player3Id,
//      team2player4Id = team2player4Id,
//      team2player5Id = team2player5Id,
//      team1Points = team1Points + other.team1Points,
//      team1ExpectedPoints = team1ExpectedPoints + other.team1ExpectedPoints,
//      team2Points = team2Points + other.team2Points,
//      team2ExpectedPoints = team2ExpectedPoints + other.team2ExpectedPoints,
//      team1Assists = team1Assists + other.team1Assists,
//      team2Assists = team2Assists + other.team2Assists,
//      team1DefensiveRebounds = team1DefensiveRebounds + other.team1DefensiveRebounds,
//      team2DefensiveRebounds = team2DefensiveRebounds + other.team2DefensiveRebounds,
//      team1OffensiveRebounds = team1OffensiveRebounds + other.team1OffensiveRebounds,
//      team2OffensiveRebounds = team2OffensiveRebounds + other.team2OffensiveRebounds,
//      team1Fouls = team1Fouls + other.team1Fouls,
//      team2Fouls = team2Fouls + other.team2Fouls,
//      team1Turnovers = team1Turnovers + other.team1Turnovers,
//      team2Turnovers = team2Turnovers + other.team2Turnovers,
//      team1FieldGoalAttempts = team1FieldGoalAttempts + other.team1FieldGoalAttempts,
//      team1FieldGoals = team1FieldGoals + other.team1FieldGoals,
//      team1ThreePtAttempts = team1ThreePtAttempts + other.team1ThreePtAttempts,
//      team1ThreePtMade = team1ThreePtMade + other.team1ThreePtMade,
//      team1FreeThrowAttempts = team1FreeThrowAttempts + other.team1FreeThrowAttempts,
//      team1FreeThrowsMade = team1FreeThrowsMade + other.team1FreeThrowsMade,
//      team2FieldGoalAttempts = team2FieldGoalAttempts + other.team2FieldGoalAttempts,
//      team2FieldGoals = team2FieldGoals + other.team2FieldGoals,
//      team2ThreePtAttempts = team2ThreePtAttempts + other.team2ThreePtAttempts,
//      team2ThreePtMade = team2ThreePtMade + other.team2ThreePtMade,
//      team2FreeThrowAttempts = team2FreeThrowAttempts + other.team2FreeThrowAttempts,
//      team2FreeThrowsMade = team2FreeThrowsMade + other.team2FreeThrowsMade,
//      team1Possessions = team1Possessions + other.team1Possessions,
//      team2Possessions = team2Possessions + other.team2Possessions,
//      seconds = seconds + other.seconds
//    )
//
//  def toOneWayStints: Seq[LuckAdjustedOneWayStintWithCoach] =
//    Seq(
//      LuckAdjustedOneWayStintWithCoach(
//        primaryKey = Seq(
//          team1player1Id,
//          team1player2Id,
//          team1player3Id,
//          team1player4Id,
//          team1player5Id,
//          team2player1Id,
//          team2player2Id,
//          team2player3Id,
//          team2player4Id,
//          team2player5Id,
//          season
//        ).mkString("_"),
//        season = season,
//        seasonType = seasonType,
//        dt = dt,
//        offenseTeamId1 = teamId1,
//        offensePlayer1Id = team1player1Id,
//        offensePlayer2Id = team1player2Id,
//        offensePlayer3Id = team1player3Id,
//        offensePlayer4Id = team1player4Id,
//        offensePlayer5Id = team1player5Id,
//        defenseTeamId2 = teamId2,
//        defensePlayer1Id = team2player1Id,
//        defensePlayer2Id = team2player2Id,
//        defensePlayer3Id = team2player3Id,
//        defensePlayer4Id = team2player4Id,
//        defensePlayer5Id = team2player5Id,
//        points = team1Points,
//        expectedPoints = team1ExpectedPoints,
//
//        defensiveRebounds = team1DefensiveRebounds,
//        offensiveRebounds = team1OffensiveRebounds,
//
//        opponentDefensiveRebounds = team2DefensiveRebounds,
//        opponentOffensiveRebounds = team2OffensiveRebounds,
//
//        turnovers = team1Turnovers,
//
//        fieldGoalAttempts = team1FieldGoalAttempts,
//        fieldGoals = team1FieldGoals,
//        threePtAttempts = team1ThreePtAttempts,
//        threePtMade = team1ThreePtMade,
//        freeThrowAttempts = team1FreeThrowAttempts,
//        freeThrowsMade = team1FreeThrowsMade,
//
//        possessions = team1Possessions,
//        seconds = seconds
//      ),
//      LuckAdjustedOneWayStintWithCoach(
//        primaryKey = Seq(
//          team2player1Id,
//          team2player2Id,
//          team2player3Id,
//          team2player4Id,
//          team2player5Id,
//          team1player1Id,
//          team1player2Id,
//          team1player3Id,
//          team1player4Id,
//          team1player5Id,
//          season
//        ).mkString("_"),
//        season = season,
//        seasonType = seasonType,
//        dt = dt,
//        offenseTeamId1 = teamId2,
//        offensePlayer1Id = team2player1Id,
//        offensePlayer2Id = team2player2Id,
//        offensePlayer3Id = team2player3Id,
//        offensePlayer4Id = team2player4Id,
//        offensePlayer5Id = team2player5Id,
//        defenseTeamId2 = teamId1,
//        defensePlayer1Id = team1player1Id,
//        defensePlayer2Id = team1player2Id,
//        defensePlayer3Id = team1player3Id,
//        defensePlayer4Id = team1player4Id,
//        defensePlayer5Id = team1player5Id,
//        points = team2Points,
//        expectedPoints = team2ExpectedPoints,
//        defensiveRebounds = team2DefensiveRebounds,
//        offensiveRebounds = team2OffensiveRebounds,
//
//        opponentDefensiveRebounds = team1DefensiveRebounds,
//        opponentOffensiveRebounds = team1OffensiveRebounds,
//
//        turnovers = team2Turnovers,
//
//        fieldGoalAttempts = team2FieldGoalAttempts,
//        fieldGoals = team2FieldGoals,
//        threePtAttempts = team2ThreePtAttempts,
//        threePtMade = team2ThreePtMade,
//        freeThrowAttempts = team2FreeThrowAttempts,
//        freeThrowsMade = team2FreeThrowsMade,
//        possessions = team2Possessions,
//        seconds = seconds
//      )
//    )
//}
//
//final case class LuckAdjustedOneWayStintWithCoach(primaryKey: String,
//                                         season: String,
//                                         seasonType: String,
//                                         dt: String,
//                                         offenseTeamId1: jl.Integer,
//                                         offensePlayer1Id: jl.Integer,
//                                         offensePlayer2Id: jl.Integer,
//                                         offensePlayer3Id: jl.Integer,
//                                         offensePlayer4Id: jl.Integer,
//                                         offensePlayer5Id: jl.Integer,
//                                         defenseTeamId2: jl.Integer,
//                                         defensePlayer1Id: jl.Integer,
//                                         defensePlayer2Id: jl.Integer,
//                                         defensePlayer3Id: jl.Integer,
//                                         defensePlayer4Id: jl.Integer,
//                                         defensePlayer5Id: jl.Integer,
//                                         points: jl.Double = 0.0,
//                                         expectedPoints: jl.Double = 0.0,
//                                         defensiveRebounds: jl.Integer = 0,
//                                         offensiveRebounds: jl.Integer = 0,
//                                         opponentDefensiveRebounds: jl.Integer = 0,
//                                         opponentOffensiveRebounds: jl.Integer = 0,
//                                         offensiveFouls: jl.Integer = 0,
//                                         defensiveFouls: jl.Integer = 0,
//                                         turnovers: jl.Integer = 0,
//                                         fieldGoalAttempts: jl.Integer = 0,
//                                         fieldGoals: jl.Integer = 0,
//                                         threePtAttempts: jl.Integer = 0,
//                                         threePtMade: jl.Integer = 0,
//                                         freeThrowAttempts: jl.Integer = 0,
//                                         freeThrowsMade: jl.Integer = 0,
//                                         possessions: jl.Integer = 0,
//                                         seconds: jl.Integer = 0) {
//
//  def +(other: LuckAdjustedOneWayStintWithCoach): LuckAdjustedOneWayStintWithCoach =
//    LuckAdjustedOneWayStintWithCoach(
//      primaryKey = primaryKey,
//      season = season,
//      seasonType = seasonType,
//      dt = dt,
//      offenseTeamId1 = offenseTeamId1,
//      offensePlayer1Id = offensePlayer1Id,
//      offensePlayer2Id = offensePlayer2Id,
//      offensePlayer3Id = offensePlayer3Id,
//      offensePlayer4Id = offensePlayer4Id,
//      offensePlayer5Id = offensePlayer5Id,
//      defenseTeamId2 = defenseTeamId2,
//      defensePlayer1Id = defensePlayer1Id,
//      defensePlayer2Id = defensePlayer2Id,
//      defensePlayer3Id = defensePlayer3Id,
//      defensePlayer4Id = defensePlayer4Id,
//      defensePlayer5Id = defensePlayer5Id,
//      points = points + other.points,
//      expectedPoints = expectedPoints + other.expectedPoints,
//
//      opponentDefensiveRebounds = opponentDefensiveRebounds + other.opponentDefensiveRebounds,
//      opponentOffensiveRebounds = opponentOffensiveRebounds + other.opponentOffensiveRebounds,
//
//      defensiveRebounds = defensiveRebounds + other.defensiveRebounds,
//      offensiveRebounds = offensiveRebounds + other.offensiveRebounds,
//
//      offensiveFouls = offensiveFouls + other.offensiveFouls,
//      defensiveFouls = defensiveFouls + other.defensiveFouls,
//
//      turnovers = turnovers + other.turnovers,
//
//      fieldGoalAttempts = fieldGoalAttempts + other.fieldGoalAttempts,
//      fieldGoals = fieldGoals + other.fieldGoals,
//      threePtAttempts = threePtAttempts + other.threePtAttempts,
//      threePtMade = threePtMade + other.threePtMade,
//      freeThrowAttempts = freeThrowAttempts + other.freeThrowAttempts,
//      freeThrowsMade = freeThrowsMade + other.freeThrowsMade,
//
//      possessions = possessions + other.possessions,
//      seconds = seconds + other.seconds
//    )
//}
