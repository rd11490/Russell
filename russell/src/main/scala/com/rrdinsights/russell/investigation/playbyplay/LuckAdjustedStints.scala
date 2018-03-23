package com.rrdinsights.russell.investigation.playbyplay

import java.{lang => jl}

import com.rrdinsights.russell.investigation.shots.ShotUtils
import com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedPointsArguments
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel._
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.{MapJoin, TimeUtils}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object LuckAdjustedStints {
  /**
    * This object is for calculating stints for luck adjusted RAPM
    *
    */
  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow
    val args = ExpectedPointsArguments(strings)
    val season = args.season
    //val season = "2017-18"

    val whereSeason = s"season = '$season'"

    val playByPlay = readPlayByPlay(whereSeason)

    println(s"${playByPlay.size} PlayByPlay Events <<<<<<<<<<<<<<<<<<")

    val freeThrowMap = buildPlayerCareerFreeThrowPercentMap()

    val seasonShots = ShotUtils.readScoredShots(whereSeason)

    val keyedPbP = playByPlay.map(v => ((v.gameId, v.eventNumber), v)).toMap
    val keyedShots = seasonShots.map(v => ((v.gameId, v.eventNumber), v)).toMap


    val playByPlayWithShotData = MapJoin.leftOuterJoin(keyedPbP, keyedShots)

    val stints = playByPlayWithShotData
      .groupBy(v => (v._1.gameId, v._1.period))
      .flatMap(v => seperatePossessions(v._2.sortBy(_._1)))
      .map(v => parsePossesions(v, freeThrowMap, dt))
      .groupBy(_.primaryKey)
      .map(v => v._2.reduce(_ + _))
      .toSeq

    writeStints(stints)

    val oneWayStints = stints.flatMap(_.toOneWayStints).groupBy(_.primaryKey).map(v => v._2.reduce(_ + _)).toSeq

    writeOneWayStints(oneWayStints)

    val secondsPlayed = stints
      .flatMap(v => {
        Seq(v.team1player1Id,
          v.team1player2Id,
          v.team1player3Id,
          v.team1player4Id,
          v.team1player5Id,
          v.team2player1Id,
          v.team2player2Id,
          v.team2player3Id,
          v.team2player4Id,
          v.team2player5Id)
          .map(i => SecondsPlayedContainer(s"${i}_${v.season}", i, v.seconds, v.season))
      })
      .groupBy(_.primaryKey)
      .map(v => v._2.reduce(_ + _))
      .toSeq

    writeSecondsPlayed(secondsPlayed)
  }

  private def writeStints(stints: Seq[LuckAdjustedStint]): Unit = {
    MySqlClient.createTable(NBATables.luck_adjusted_stints)
    MySqlClient.insertInto(NBATables.luck_adjusted_stints, stints)
  }

  private def writeOneWayStints(stints: Seq[LuckAdjustedOneWayStint]): Unit = {
    MySqlClient.createTable(NBATables.luck_adjusted_one_way_stints)
    MySqlClient.insertInto(NBATables.luck_adjusted_one_way_stints, stints)
  }

  private def writeSecondsPlayed(secondsPlayed: Seq[SecondsPlayedContainer]): Unit = {
    MySqlClient.createTable(NBATables.seconds_played)
    MySqlClient.insertInto(NBATables.seconds_played, secondsPlayed)
  }

  private def parsePossesions(eventsWithTime: (Seq[(PlayByPlayWithLineup, Option[ScoredShot])], Integer),
                              freeThrowMap: Map[jl.Integer, jl.Double],
                              dt: String): LuckAdjustedStint = {

    val events = eventsWithTime._1
    val time = eventsWithTime._2
    val points = countPoints(events, freeThrowMap)

    val first = events.head
    val team1Points = points.find(_.teamId == first._1.teamId1)
    val team2Points = points.find(_.teamId == first._1.teamId2)
    val primaryKey = Seq(
      first._1.team1player1Id,
      first._1.team1player2Id,
      first._1.team1player3Id,
      first._1.team1player4Id,
      first._1.team1player5Id,
      first._1.team2player1Id,
      first._1.team2player2Id,
      first._1.team2player3Id,
      first._1.team2player4Id,
      first._1.team2player5Id,
      first._1.season).mkString("_")

    val possesion = determinePossession(events)
    val team1Possessions = if (possesion == first._1.teamId1) 1 else 0
    val team2Possessions = if (possesion == first._1.teamId2) 1 else 0

    LuckAdjustedStint(
      primaryKey = primaryKey,
      season = first._1.season,
      dt = dt,
      teamId1 = first._1.teamId1,
      team1player1Id = first._1.team1player1Id,
      team1player2Id = first._1.team1player2Id,
      team1player3Id = first._1.team1player3Id,
      team1player4Id = first._1.team1player4Id,
      team1player5Id = first._1.team1player5Id,
      teamId2 = first._1.teamId2,
      team2player1Id = first._1.team2player1Id,
      team2player2Id = first._1.team2player2Id,
      team2player3Id = first._1.team2player3Id,
      team2player4Id = first._1.team2player4Id,
      team2player5Id = first._1.team2player5Id,

      team1Points = team1Points.map(_.points).getOrElse(0.0),
      team1ExpectedPoints = team1Points.map(_.expectedPoints).getOrElse(0.0),

      team2Points = team2Points.map(_.points).getOrElse(0.0),
      team2ExpectedPoints = team2Points.map(_.expectedPoints).getOrElse(0.0),
      team1Possessions = team1Possessions,
      team2Possessions = team2Possessions,
      seconds = time)
  }

  private def determinePossession(events: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Integer = {
    val techFouls = Seq(11, 14, 15, 17)
    events.flatMap(v => v._2).headOption.map(_.offenseTeamId).getOrElse(
      events.find(v => {
        v._1.playType == PlayByPlayEventMessageType.Make.toString ||
          v._1.playType == PlayByPlayEventMessageType.Miss.toString ||
          v._1.playType == PlayByPlayEventMessageType.FreeThrow.toString ||
          v._1.playType == PlayByPlayEventMessageType.Turnover.toString
      })
        .map(_._1.player1TeamId)
        .getOrElse(
          if (PlayByPlayEventMessageType.valueOf(events.last._1.playType) == PlayByPlayEventMessageType.Foul &&
            techFouls.contains(events.last._1.eventActionType)) {
            if (events.last._1.player1TeamId == events.last._1.teamId1) {
              events.last._1.teamId2
            } else {
              events.last._1.teamId1
            }
          } else {
            events.last._1.player1TeamId
          }))
  }

  private def countPoints(events: Seq[(PlayByPlayWithLineup, Option[ScoredShot])], freeThrowMap: Map[Integer, jl.Double]): Seq[TeamPoints] = {
    events.filter(v => v._1.playType == PlayByPlayEventMessageType.Make.toString ||
      (v._1.playType == PlayByPlayEventMessageType.FreeThrow.toString))
      .map(v => PlayByPlayEventMessageType.valueOf(v._1.playType) match {
        case PlayByPlayEventMessageType.FreeThrow => buildStintFromFreeThrow(v, freeThrowMap)
        case PlayByPlayEventMessageType.Make => buildStingFromShot(v)
        case PlayByPlayEventMessageType.Miss => buildStingFromShot(v)
      })
      .groupBy(_.teamId)
      .map(v => v._2.reduce(_ + _))
      .toSeq
  }

  private def buildStingFromShot(event: (PlayByPlayWithLineup, Option[ScoredShot])): TeamPoints = {
    try {
      val teamId = event._1.player1TeamId
      val scoredShot = event._2.get
      val points = scoredShot.shotValue.toDouble * scoredShot.shotMade
      val expectedPoints = if (scoredShot.shotValue == 3) scoredShot.expectedPoints.doubleValue() else points
      TeamPoints(teamId, points, expectedPoints)
    } catch {
      case e: Throwable =>
        println(event)
        throw e
    }

  }

  private def buildStintFromFreeThrow(event: (PlayByPlayWithLineup, Option[ScoredShot]), freeThrowMap: Map[Integer, jl.Double]): TeamPoints = {
    val teamId = event._1.player1TeamId
    val expectedPoints = freeThrowMap(event._1.player1Id).doubleValue()
    val points = if (checkIfFTMade(event)) 1.0 else 0.0
    TeamPoints(teamId, points, expectedPoints)
  }

  //  private def countTurnovers(events: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): (Int, Int) = {
  //    //    '0' No TO - P1 is player who "NO TOed it"
  //    //    '1' Steal - P1 is player who lost ball, P2 is player who stole it
  //    //    '2' Steal - P1 is player who lost ball, P2 is player who stole it
  //    //    '4' Travel - P1 is player who traveled
  //    //    '6' Double Dribble - P1 committed TO
  //    //    '7' Discontinued Dribble - P1 committed TO
  //    //    '8' 3 Second Violation - P1 committed TO
  //    //    '9'
  //    //    '10'
  //    //    '11'
  //    //    '12'
  //    //    '13'
  //    //    '15'
  //    //    '17'
  //    //    '18'
  //    //    '19'
  //    //    '20'
  //    //    '21'
  //    //    '33'
  //    //    '35'
  //    //    '36'
  //    //    '37'
  //    //    '39'
  //    //    '40'
  //    //    '44'
  //    //    '45'
  //
  //  }

  private[playbyplay] def seperatePossessions(events: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Seq[(Seq[(PlayByPlayWithLineup, Option[ScoredShot])], Integer)] = {
    val possessions: ArrayBuffer[(ArrayBuffer[(PlayByPlayWithLineup, Option[ScoredShot])], Integer)] =
      new ArrayBuffer[(ArrayBuffer[(PlayByPlayWithLineup, Option[ScoredShot])], Integer)]()
    var i: Int = 0
    var time: Int = TimeUtils.timeFromStartOfGameAtPeriod(events.head._1.period)
    var possession: mutable.ArrayBuffer[(PlayByPlayWithLineup, Option[ScoredShot])] = new ArrayBuffer[(PlayByPlayWithLineup, Option[ScoredShot])]()
    while (i < events.size) {
      val event = events(i)
      if (isEndOfPossesion(event, possession)) {
        possessions.append((possession, possession.last._1.timeElapsed - time))
        time = possession.last._1.timeElapsed
        possession = new ArrayBuffer[(PlayByPlayWithLineup, Option[ScoredShot])]()
      }
      possession.append(event)
      i += 1
    }
    possessions
  }

  private[playbyplay] def isEndOfPossesion(event: (PlayByPlayWithLineup, Option[ScoredShot]),
                                           lastEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    lastEvents.nonEmpty &&
      (isTechnicalFT(event) ||
        isEndOfPeriod(event) || (
        hasTimeChanged(event, lastEvents) &&
          (isMakeAndNotAndOne(event, lastEvents) ||
            isDefensiveRebound(event, lastEvents) ||
            isLastMadeFreeThrow(lastEvents) ||
            isTurnover(lastEvents))))

  }

  private def isTimeout(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean = {
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.Timeout
  }

  private def isEndOfPeriod(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean = {
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.EndOfPeriod
  }

  private def hasTimeChanged(event: (PlayByPlayWithLineup, Option[ScoredShot]),
                             lastEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    event._1.timeElapsed != lastEvents.last._1.timeElapsed
  }

  private def isMakeAndNotAndOne(event: (PlayByPlayWithLineup, Option[ScoredShot]),
                                 lastEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    val lastEvent = lastEvents.last
    PlayByPlayEventMessageType.valueOf(lastEvent._1.playType) == PlayByPlayEventMessageType.Make &&
      !(PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.Foul &&
        event._1.timeElapsed == lastEvent._1.timeElapsed)


  }

  private def isDefensiveRebound(event: (PlayByPlayWithLineup, Option[ScoredShot]),
                                 lastEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    if (lastEvents.lengthCompare(1) > 0) {
      val lastEvent = lastEvents.last
      val secondToLastEvent = lastEvents.takeRight(2).head
      PlayByPlayEventMessageType.valueOf(lastEvent._1.playType) == PlayByPlayEventMessageType.Rebound &&
        (PlayByPlayEventMessageType.valueOf(secondToLastEvent._1.playType) == PlayByPlayEventMessageType.Miss ||
          PlayByPlayEventMessageType.valueOf(secondToLastEvent._1.playType) == PlayByPlayEventMessageType.FreeThrow) &&
        lastEvent._1.player1TeamId != secondToLastEvent._1.player1TeamId
    } else {
      false
    }
  }

  private def isLastMadeFreeThrow(lastEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    val lastEvent = lastEvents.last

    lastEvent._1.eventActionType == 16 ||
      ((lastEvent._1.eventActionType == 15 ||
        lastEvent._1.eventActionType == 12 ||
        lastEvent._1.eventActionType == 10) &&
        checkIfFTMade(lastEvent))
  }

  private def isTechnicalFT(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.FreeThrow &&
      event._1.eventActionType == 16


  private def isTurnover(lastEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    PlayByPlayEventMessageType.valueOf(lastEvents.last._1.playType) == PlayByPlayEventMessageType.Turnover
  }

  private[playbyplay] def checkIfFTMade(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    !Option(event._1.awayDescription).exists(_.contains("MISS")) &&
      !Option(event._1.homeDescription).exists(_.contains("MISS"))

  private def readPlayByPlay(season: String): Seq[PlayByPlayWithLineup] =
    MySqlClient.selectFrom(NBATables.play_by_play_with_lineup, PlayByPlayWithLineup.apply, season)

  private def buildPlayerCareerFreeThrowPercentMap(/*IO*/): Map[Integer, jl.Double] =
    MySqlClient.selectFrom(NBATables.raw_player_profile_career_totals, RawPlayerProfileCareer.apply)
      .map(v => (v.playerId, v.freeThrowPercent))
      .toMap

}

final case class LuckAdjustedStint(primaryKey: String,
                                   season: String,
                                   dt: String,

                                   teamId1: jl.Integer,
                                   team1player1Id: jl.Integer,
                                   team1player2Id: jl.Integer,
                                   team1player3Id: jl.Integer,
                                   team1player4Id: jl.Integer,
                                   team1player5Id: jl.Integer,
                                   teamId2: jl.Integer,
                                   team2player1Id: jl.Integer,
                                   team2player2Id: jl.Integer,
                                   team2player3Id: jl.Integer,
                                   team2player4Id: jl.Integer,
                                   team2player5Id: jl.Integer,

                                   team1Points: jl.Double = 0.0,
                                   team1ExpectedPoints: jl.Double = 0.0,
                                   team2Points: jl.Double = 0.0,
                                   team2ExpectedPoints: jl.Double = 0.0,

                                   team1Assists: jl.Integer = 0,
                                   team2Assists: jl.Integer = 0,

                                   team1DefensiveRebounds: jl.Integer = 0,
                                   team2DefensiveRebounds: jl.Integer = 0,

                                   team1OffensiveRebounds: jl.Integer = 0,
                                   team2OffensiveRebounds: jl.Integer = 0,

                                   team1Fouls: jl.Integer = 0,
                                   team2Fouls: jl.Integer = 0,

                                   team1Turnovers: jl.Integer = 0,
                                   team2Turnovers: jl.Integer = 0,

                                   team1Possessions: jl.Integer = 0,
                                   team2Possessions: jl.Integer = 0,

                                   seconds: jl.Integer = 0) {
  def +(other: LuckAdjustedStint): LuckAdjustedStint =
    LuckAdjustedStint(
      primaryKey,
      season,
      dt,

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

      team1Points + other.team1Points,
      team1ExpectedPoints + other.team1ExpectedPoints,
      team2Points + other.team2Points,
      team2ExpectedPoints + other.team2ExpectedPoints,

      team1Assists + other.team1Assists,
      team2Assists + other.team2Assists,

      team1DefensiveRebounds + other.team1DefensiveRebounds,
      team2DefensiveRebounds + other.team2DefensiveRebounds,

      team1OffensiveRebounds + other.team1OffensiveRebounds,
      team2OffensiveRebounds + other.team2OffensiveRebounds,

      team1Fouls + other.team1Fouls,
      team2Fouls + other.team2Fouls,

      team1Turnovers + other.team1Turnovers,
      team2Turnovers + other.team2Turnovers,

      team1Possessions + other.team1Possessions,
      team2Possessions + other.team2Possessions,

      seconds + other.seconds)

  def toOneWayStints: Seq[LuckAdjustedOneWayStint] =
    Seq(
      LuckAdjustedOneWayStint(
        primaryKey = Seq(
          team1player1Id, team1player2Id, team1player3Id, team1player4Id, team1player5Id,
          team2player1Id, team2player2Id, team2player3Id, team2player4Id, team2player5Id,
          season).mkString("_"),
        season = season,
        dt = dt,
        offenseTeamId1 = teamId1,
        offensePlayer1Id = team1player1Id,
        offensePlayer2Id = team1player2Id,
        offensePlayer3Id = team1player3Id,
        offensePlayer4Id = team1player4Id,
        offensePlayer5Id = team1player5Id,

        defenseTeamId2 = teamId2,
        defensePlayer1Id = team2player1Id,
        defensePlayer2Id = team2player2Id,
        defensePlayer3Id = team2player3Id,
        defensePlayer4Id = team2player4Id,
        defensePlayer5Id = team2player5Id,

        points = team1Points,
        expectedPoints = team1ExpectedPoints,
        defensiveRebounds = team2DefensiveRebounds,
        offensiveRebounds = team1OffensiveRebounds,

        possessions = team1Possessions,
        seconds = seconds),
      LuckAdjustedOneWayStint(
        primaryKey = Seq(
          team2player1Id, team2player2Id, team2player3Id, team2player4Id, team2player5Id,
          team1player1Id, team1player2Id, team1player3Id, team1player4Id, team1player5Id,
          season).mkString("_"),
        season = season,
        dt = dt,
        offenseTeamId1 = teamId2,
        offensePlayer1Id = team2player1Id,
        offensePlayer2Id = team2player2Id,
        offensePlayer3Id = team2player3Id,
        offensePlayer4Id = team2player4Id,
        offensePlayer5Id = team2player5Id,

        defenseTeamId2 = teamId1,
        defensePlayer1Id = team1player1Id,
        defensePlayer2Id = team1player2Id,
        defensePlayer3Id = team1player3Id,
        defensePlayer4Id = team1player4Id,
        defensePlayer5Id = team1player5Id,

        points = team2Points,
        expectedPoints = team2ExpectedPoints,
        defensiveRebounds = team1DefensiveRebounds,
        offensiveRebounds = team2OffensiveRebounds,

        possessions = team2Possessions,
        seconds = seconds))
}

private final case class TeamPoints(teamId: Integer, points: jl.Double, expectedPoints: jl.Double) {
  def +(other: TeamPoints): TeamPoints =
    TeamPoints(teamId, points + other.points, expectedPoints + other.expectedPoints)
}

final case class SecondsPlayedContainer(primaryKey: String, playerId: jl.Integer, secondsPlayed: jl.Integer, season: String) {
  def +(other: SecondsPlayedContainer): SecondsPlayedContainer =
    SecondsPlayedContainer(primaryKey, playerId, secondsPlayed + other.secondsPlayed, season)
}

final case class LuckAdjustedOneWayStint(
                                          primaryKey: String,
                                          season: String,
                                          dt: String,

                                          offenseTeamId1: jl.Integer,
                                          offensePlayer1Id: jl.Integer,
                                          offensePlayer2Id: jl.Integer,
                                          offensePlayer3Id: jl.Integer,
                                          offensePlayer4Id: jl.Integer,
                                          offensePlayer5Id: jl.Integer,
                                          defenseTeamId2: jl.Integer,
                                          defensePlayer1Id: jl.Integer,
                                          defensePlayer2Id: jl.Integer,
                                          defensePlayer3Id: jl.Integer,
                                          defensePlayer4Id: jl.Integer,
                                          defensePlayer5Id: jl.Integer,

                                          points: jl.Double = 0.0,
                                          expectedPoints: jl.Double = 0.0,

                                          defensiveRebounds: jl.Integer = 0,

                                          offensiveRebounds: jl.Integer = 0,

                                          offensiveFouls: jl.Integer = 0,
                                          defensiveFouls: jl.Integer = 0,

                                          turnovers: jl.Integer = 0,
                                          possessions: jl.Integer = 0,
                                          seconds: jl.Integer = 0) {

  def +(other: LuckAdjustedOneWayStint): LuckAdjustedOneWayStint =
    LuckAdjustedOneWayStint(
      primaryKey,
      season,
      dt,

      offenseTeamId1,
      offensePlayer1Id,
      offensePlayer2Id,
      offensePlayer3Id,
      offensePlayer4Id,
      offensePlayer5Id,
      defenseTeamId2,
      defensePlayer1Id,
      defensePlayer2Id,
      defensePlayer3Id,
      defensePlayer4Id,
      defensePlayer5Id,

      points + other.points,
      expectedPoints + other.expectedPoints,

      defensiveRebounds + other.defensiveRebounds,

      offensiveRebounds + other.offensiveRebounds,

      offensiveFouls + other.offensiveFouls,
      defensiveFouls + other.defensiveFouls,

      turnovers + other.turnovers,
      possessions + other.possessions,
      seconds + other.seconds
    )
}