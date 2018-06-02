package com.rrdinsights.russell.investigation.playbyplay.luckadjusted

import java.{lang => jl}

import com.rrdinsights.russell.investigation.playbyplay.PlayByPlayUtils
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{
  PlayByPlayEventMessageType,
  PlayByPlayWithLineup,
  RawPlayerProfileCareer,
  ScoredShot
}
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.TimeUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object LuckAdjustedUtils {

  def determinePossession(
                           events: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Integer = {
    val techFouls = Seq(11, 14, 15, 17)
    events
      .flatMap(v => v._2)
      .headOption
      .map(_.offenseTeamId)
      .getOrElse(events
        .find(v => {
          v._1.playType == PlayByPlayEventMessageType.Make.toString ||
            v._1.playType == PlayByPlayEventMessageType.Miss.toString ||
            v._1.playType == PlayByPlayEventMessageType.FreeThrow.toString ||
            v._1.playType == PlayByPlayEventMessageType.Turnover.toString
        })
        .map(_._1.player1TeamId)
        .getOrElse(if (PlayByPlayEventMessageType.valueOf(
          events.last._1.playType) == PlayByPlayEventMessageType.Foul &&
          techFouls.contains(events.last._1.eventActionType)) {
          if (events.last._1.player1TeamId == events.last._1.teamId1) {
            events.last._1.teamId2
          } else {
            events.last._1.teamId1
          }
        } else {
          0
        }))
  }

  def countPoints(events: Seq[(PlayByPlayWithLineup, Option[ScoredShot])],
                  freeThrowMap: Map[Integer, jl.Double]): Seq[TeamPoints] = {
    events
      .filter(v => isShotOrFreeThrow(v))
      .map(v =>
        PlayByPlayEventMessageType.valueOf(v._1.playType) match {
          case PlayByPlayEventMessageType.FreeThrow =>
            buildStintFromFreeThrow(v, freeThrowMap)
          case PlayByPlayEventMessageType.Make => buildStintFromShot(v)
          case PlayByPlayEventMessageType.Miss => buildStintFromShot(v)
        })
      .groupBy(_.teamId)
      .map(v => v._2.reduce(_ + _))
      .toSeq
  }

  private def isShotOrFreeThrow(
                                 event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) match {
      case PlayByPlayEventMessageType.FreeThrow => true
      case PlayByPlayEventMessageType.Make => true
      case PlayByPlayEventMessageType.Miss => true
      case _ => false
    }

  def isThreePointShot(scoredShot: ScoredShot): Boolean =
    scoredShot.shotValue == 3

  def isMadeShot(scoredShot: ScoredShot): Boolean =
    scoredShot.shotMade == 1

  def isMake(event: PlayByPlayWithLineup): Boolean =
    PlayByPlayEventMessageType.valueOf(event.playType) == PlayByPlayEventMessageType.Make

  def buildStintFromShot(
                          event: (PlayByPlayWithLineup, Option[ScoredShot])): TeamPoints = {
    try {
      val teamId = event._1.player1TeamId
      event._2
        .map(shot => {
          val points = shot.shotValue.toDouble * shot.shotMade
          val expectedPoints =
            if (isThreePointShot(shot)) shot.expectedPoints.doubleValue()
            else points
          val fieldGoals = if (isMadeShot(shot)) 1 else 0
          val threePtAttempts = if (isThreePointShot(shot)) 1 else 0
          val threePtMade =
            if (isThreePointShot(shot) && isMadeShot(shot)) 1 else 0
          TeamPoints(teamId,
            points,
            expectedPoints,
            fieldGoalAttempts = 1,
            fieldGoals = fieldGoals,
            threePtAttempts = threePtAttempts,
            threePtMade = threePtMade)
        })
        .getOrElse(countPointsFromEvent(event._1))
    } catch {
      case e: Throwable =>
        println(event)
        throw e
    }

  }

  /**
    * This is only for handling issues of back input from playbyplay
    */
  def countPointsFromEvent(event: PlayByPlayWithLineup): TeamPoints = {
    println(event)
    val make = isMake(event)
    val points = if (make) 2.0 else 0.0
    val expectedPoints = if (make) 2.0 else 0.0
    val fieldGoals = if (make) 1 else 0
    val threePtAttempts = 0
    val threePtMade = 0
    TeamPoints(event.player1TeamId,
      points,
      expectedPoints,
      fieldGoalAttempts = 1,
      fieldGoals = fieldGoals,
      threePtAttempts = threePtAttempts,
      threePtMade = threePtMade)
  }

  def buildStintFromFreeThrow(
                               event: (PlayByPlayWithLineup, Option[ScoredShot]),
                               freeThrowMap: Map[Integer, jl.Double]): TeamPoints = {
    val teamId = event._1.player1TeamId
    val expectedPoints = freeThrowMap(event._1.player1Id).doubleValue()
    val points = if (PlayByPlayUtils.isMadeFreeThrow(event)) 1.0 else 0.0
    TeamPoints(teamId,
      points,
      expectedPoints,
      freeThrowAttempts = 1,
      freeThrowsMade = points.intValue())
  }

  //  def countTurnovers(events: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): (Int, Int) = {
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

  def seperatePossessions(
                           events: Seq[(PlayByPlayWithLineup, Option[ScoredShot])])
  : Seq[(Seq[(PlayByPlayWithLineup, Option[ScoredShot])], Integer)] = {
    val sortedEvents = events.sortBy(v => (v._1.timeElapsed, v._1.eventNumber))
    val possessions: ArrayBuffer[
      (ArrayBuffer[(PlayByPlayWithLineup, Option[ScoredShot])], Integer)] =
      new ArrayBuffer[(ArrayBuffer[(PlayByPlayWithLineup, Option[ScoredShot])],
        Integer)]()
    var i: Int = 0
    var time: Int = TimeUtils.timeFromStartOfGameAtPeriod(sortedEvents.head._1.period)
    var possession: mutable.ArrayBuffer[(PlayByPlayWithLineup, Option[ScoredShot])] =
      new ArrayBuffer[(PlayByPlayWithLineup, Option[ScoredShot])]()
    while (i < sortedEvents.size) {
      val event = sortedEvents(i)
      possession.append(event)
      if (isEndOfPossesion(event, sortedEvents, possession.size)) {
        possessions.append((possession, possession.last._1.timeElapsed - time))
        time = possession.last._1.timeElapsed
        possession = new ArrayBuffer[(PlayByPlayWithLineup, Option[ScoredShot])]()
      }
      i += 1
    }
    possessions
  }

  /**
    * Possession changes on:
    * Jump ball (not first jump ball)
    * Last made freethrow (Ignore FT 1 of 1 on away from play fouls with no made shot)
    * Defensive Rebounds
    * Makes
    * Turnovers
    *
    * @param event
    * @param allEvents
    * @return
    */
  def isEndOfPossesion(
                        event: (PlayByPlayWithLineup, Option[ScoredShot]),
                        allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])],
                        possessionSize: Long): Boolean = {
    PlayByPlayUtils.isMakeAndNotAndOne(event, allEvents) ||
      PlayByPlayUtils.isDefensiveRebound(event, allEvents) ||
      PlayByPlayUtils.isLastMadeFreeAndEndOfPoss(event, allEvents) ||
      PlayByPlayUtils.isTurnover(event, allEvents) ||
      (PlayByPlayUtils.isJumpBall(event, allEvents) && possessionSize > 1)

  }

  def readPlayByPlay(where: String*): Seq[PlayByPlayWithLineup] =
    MySqlClient.selectFrom(NBATables.play_by_play_with_lineup,
      PlayByPlayWithLineup.apply,
      where: _*)

  def buildPlayerCareerFreeThrowPercentMap(/*IO*/): Map[Integer, jl.Double] =
    MySqlClient
      .selectFrom(NBATables.raw_player_profile_career_totals,
        RawPlayerProfileCareer.apply)
      .map(v => (v.playerId, v.freeThrowPercent))
      .toMap
}

final case class TeamPoints(teamId: Integer,
                            points: jl.Double,
                            expectedPoints: jl.Double,
                            fieldGoalAttempts: jl.Integer = 0,
                            fieldGoals: jl.Integer = 0,
                            threePtAttempts: jl.Integer = 0,
                            threePtMade: jl.Integer = 0,
                            freeThrowAttempts: jl.Integer = 0,
                            freeThrowsMade: jl.Integer = 0) {
  def +(other: TeamPoints): TeamPoints =
    TeamPoints(
      teamId,
      points + other.points,
      expectedPoints + other.expectedPoints,
      fieldGoalAttempts + other.fieldGoalAttempts,
      fieldGoals + other.fieldGoals,
      threePtAttempts + other.threePtAttempts,
      threePtMade + other.threePtMade,
      freeThrowAttempts + other.freeThrowAttempts,
      freeThrowsMade + other.freeThrowsMade
    )
}

final case class MissingEventException(private val message: String = "")
  extends Exception(message)
