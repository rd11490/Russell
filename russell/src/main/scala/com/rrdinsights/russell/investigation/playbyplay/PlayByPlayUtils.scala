package com.rrdinsights.russell.investigation.playbyplay

import com.rrdinsights.russell.storage.datamodel.{
  PlayByPlayEventMessageType,
  PlayByPlayWithLineup,
  ScoredShot
}

object PlayByPlayUtils {

  def isTimeout(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean = {
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.Timeout
  }

  def isEndOfPeriod(
      event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean = {
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.EndOfPeriod
  }

  def hasTimeChanged(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      lastEvent: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean = {
    event._1.timeElapsed != lastEvent._1.timeElapsed
  }

  def isJumpBall(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.JumpBall &&
    event._1.timeElapsed != 0 &&
    event._1.timeElapsed != 60 * 12 * 4
  }

  def isMakeAndNotAndOne(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {

    isMake(event) && !isAnd1(event, allEvents)
  }

  def isAnd1(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    allEvents
      .withFilter(v => v._1.timeElapsed <= event._1.timeElapsed+5 && v._1.timeElapsed >= event._1.timeElapsed)
      .withFilter(v => isFoul(v) || isFreeThrow1of1(v))
      .map {
        case c if isFoul(c) =>
          c._1.player2Id == event._1.player1Id &&
            !isTechnicalFoul(c) && !isLooseBallFoul(c) && !isInboundFoul(c)
        case c if isFreeThrow1of1(c) =>
          c._1.player1Id == event._1.player1Id
        case _ => false
      }.count(v => v) == 2
  }

  def isLastEventDefensiveRebound(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    isRebound(event) &&
    isDefensiveRebound(event, allEvents)
  }

  def isSubstitutionNotDuringFreeThrows(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      lastEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    val lastEvent = lastEvents.last
    isSubstitution(lastEvent) && !isFreeThrow(event) && !isSubstitution(event)
  }

  def isSubstitution(
      event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.Substitution

  def isShot(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    isMake(event) || isMiss(event)

  def isMake(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.Make

  def isMiss(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.Miss

  def isMissedFT(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.FreeThrow &&
      (Option(event._1.homeDescription).exists(_.contains("MISS")) ||
      Option(event._1.awayDescription).exists(_.contains("MISS")))

  def is3PtShot(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    try {
      event._2.exists(v => v.shotValue == 3) ||
      Option(event._1.homeDescription).exists(_.contains("3PT")) ||
      Option(event._1.awayDescription).exists(_.contains("3PT"))
    } catch {
      case e: Exception =>
        println(event)
        throw e
    }

  def isAssisted(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.Make &&
      event._1.player2Id != null &&
      event._1.player2Id != 0 &&
      event._1.player2TeamId == event._1.player1TeamId

  def isBlock(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.Miss &&
      event._1.player3Id != null &&
      event._1.player3Id != 0 &&
      event._1.player3TeamId != event._1.player1TeamId

  def isSteal(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.Turnover &&
      event._1.player2Id != null &&
      event._1.player2Id != 0 &&
      event._1.player2TeamId != event._1.player1TeamId

  def isFreeThrow(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.FreeThrow

  def isRebound(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.Rebound

  def extractMissedShotFromRebound(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      events: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): (PlayByPlayWithLineup, Option[ScoredShot]) = {

    try {
      sortEventsForRBD(event, events)
        .find(v => isMiss(v) || isMissedFT(v))
        .get
    } catch {
      case e: Exception =>
        println("Event:")
        println(event)
        println("All Events")
        events.foreach(v => println(s"${v._1.gameId} - ${v._1.eventNumber} - ${v._1.timeElapsed} - ${v._1.playType} - ${v._1.awayDescription} - ${v._1.homeDescription}"))
        println("Resorted Events")
        sortEventsForRBD(event, events).foreach(v => println(s"${v._1.gameId} - ${v._1.eventNumber} - ${v._1.timeElapsed} - ${v._1.playType} - ${v._1.awayDescription} - ${v._1.homeDescription}"))
        println("Index of event:")
        println(events.indexOf(event))
        println("Size of events: ")
        println(events.size)
        println("All Events Before: ")
        events
          .take(events.indexOf(event))
          .reverse
          .foreach(println)

        throw e
    }

  }

  def sortEventsForRBD(event: (PlayByPlayWithLineup, Option[ScoredShot]),
                 events: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Seq[(PlayByPlayWithLineup, Option[ScoredShot])] = {
    val sorted = events.sortWith((left: (PlayByPlayWithLineup, Option[ScoredShot]), right: (PlayByPlayWithLineup, Option[ScoredShot])) => {
      val timeDiff = left._1.timeElapsed - right._1.timeElapsed
      if (timeDiff > 0) {
        false
      } else if (timeDiff < 0) {
        true
      } else {
        if (left._1.eventNumber == event._1.eventNumber) {
          false
        } else if (right._1.eventNumber == event._1.eventNumber) {
          true
        } else {
          left._1.eventNumber < right._1.eventNumber
        }
      }
    })
    sorted.take(sorted.indexOf(event))
      .reverse
  }

  def isPlayerDefensiveRebound(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    if (isRebound(event)) {
      val missedShot = extractMissedShotFromRebound(event, allEvents)
      event._1.player1TeamId != null &&
      missedShot._1.player1TeamId != null &&
      event._1.player1TeamId != missedShot._1.player1TeamId
    } else {
      false
    }
  }

  def isPlayerOffensiveRebound(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    if (isRebound(event)) {
      val missedShot = extractMissedShotFromRebound(event, allEvents)
      event._1.player1TeamId != null &&
      missedShot._1.player1TeamId != null &&
      event._1.player1TeamId == missedShot._1.player1TeamId
    } else {
      false
    }
  }

  def isDefensiveRebound(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean =
    isPlayerDefensiveRebound(event, allEvents) ||
      isTeamDefensiveRebound(event, allEvents)

  def isOffensiveRebound(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean =
    isPlayerOffensiveRebound(event, allEvents) || isTeamOffensiveRebound(
      event,
      allEvents)

  def isTeamRebound(
      event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    isRebound(event) && event._1.player1TeamId == null

  def isTeamDefensiveRebound(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    if (isRebound(event)) {
      val missedShot = extractMissedShotFromRebound(event, allEvents)
      isTeamRebound(event) &&
      event._1.player1Id != null &&
      missedShot._1.player1TeamId != null &&
      event._1.player1Id != missedShot._1.player1TeamId
    } else {
      false
    }

  }

  def isTeamOffensiveRebound(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    if (isRebound(event)) {
      val missedShot = extractMissedShotFromRebound(event, allEvents)
      isTeamRebound(event) &&
      event._1.player1Id != null &&
      missedShot._1.player1TeamId != null &&
      event._1.player1Id == missedShot._1.player1TeamId
    } else {
      false
    }
  }

  def isLastMadeFreeThrow(
      event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean = {
    isMadeFreeThrow(event) &&
    (isFreeThrow1of1(event) ||
    isFreeThrow2of2(event) ||
    isFreeThrow3of3(event))

    // Ignore FT 1 of 1 on away from play fouls with no made shot - away from play is msgtype 6, actiontype 6
    // Tgnore loose ball foul going for rebound on missed FT
  }

  def isLastMadeFreeAndEndOfPoss(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    isLastMadeFreeThrow(event) &&
    !isAwayFromPlayOnNonMadeShot(event, allEvents) &&
    !isLooseBallFoul(event) &&
    !isLastFoulInboundFoul(event, allEvents)
  }

  def isLastFoulAwayFromPlay(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean =
    extractLastFoul(event, allEvents)
      .exists(isAwayFromPlayFoul)

  def isLastFoulAtTimeOfMadeShot(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean =
    extractLastFoul(event, allEvents)
      .exists(_._1.timeElapsed == event._1.timeElapsed)

  def isLastFoulInboundFoul(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean =
    extractLastFoul(event, allEvents)
      .exists(isInboundFoul)

  def isLastFoulLooseBall(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean =
    extractLastFoul(event, allEvents)
      .exists(isLooseBallFoul)

  def isAwayFromPlayOnNonMadeShot(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean =
    isFreeThrow1of1(event) &&
      isLastFoulAwayFromPlay(event, allEvents) &&
      isLastFoulAtTimeOfMadeShot(event, allEvents)

  def extractLastFoul(
      event: (PlayByPlayWithLineup, Option[ScoredShot]),
      allEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])])
    : Option[(PlayByPlayWithLineup, Option[ScoredShot])] =
    allEvents
      .take(allEvents.indexOf(event))
      .reverse
      .find(isFoul)

  def isTechnicalFoul(
      event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    isFoul(event) &&
      FoulType.valueOf(event._1.eventActionType) == FoulType.Technical

  def isDoubleTechnicalFoul(
      event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    isFoul(event) &&
      FoulType.valueOf(event._1.eventActionType) == FoulType.DoubleTechnical

  def isDelayOfGameFoul(
      event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    isFoul(event) &&
      FoulType.valueOf(event._1.eventActionType) == FoulType.DelayOfGame

  def isInboundFoul(
      event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    isFoul(event) &&
      FoulType.valueOf(event._1.eventActionType) == FoulType.Inbound

  def isAwayFromPlayFoul(
      event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    isFoul(event) &&
      FoulType.valueOf(event._1.eventActionType) == FoulType.AwayFromPlay

  def isLooseBallFoul(
      event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    isFoul(event) &&
      FoulType.valueOf(event._1.eventActionType) == FoulType.LooseBall

  def isFreeThrow1of1(
      event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.FreeThrow &&
      FreeThrowType.valueOf(event._1.eventActionType) == FreeThrowType.OneOfOne

  def isFreeThrow2of2(
      event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.FreeThrow &&
      FreeThrowType.valueOf(event._1.eventActionType) == FreeThrowType.TwoOfTwo

  def isFreeThrow3of3(
      event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.FreeThrow &&
      FreeThrowType.valueOf(event._1.eventActionType) == FreeThrowType.ThreeOfThree

  def isTechnicalFT(
      event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.FreeThrow &&
      event._1.eventActionType == 16

  def isTurnover(
      event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean = {
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.Turnover
  }

  def isMadeFreeThrow(
      event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    !Option(event._1.awayDescription).exists(_.contains("MISS")) &&
      !Option(event._1.homeDescription).exists(_.contains("MISS"))

  def isOffensiveFoul(
      event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    isFoul(event) && FoulType.isOffensive(event._1.eventActionType)

  def isFoul(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.Foul

}
