package com.rrdinsights.russell.investigation.playbyplay

import com.rrdinsights.russell.storage.datamodel.{PlayByPlayEventMessageType, PlayByPlayWithLineup, ScoredShot}

object PlayByPlayUtils {

  def isTimeout(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean = {
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.Timeout
  }

  def isEndOfPeriod(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean = {
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.EndOfPeriod
  }

  def hasTimeChanged(event: (PlayByPlayWithLineup, Option[ScoredShot]),
                     lastEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    event._1.timeElapsed != lastEvents.last._1.timeElapsed
  }

  def isMakeAndNotAndOne(event: (PlayByPlayWithLineup, Option[ScoredShot]),
                         lastEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    val lastEvent = lastEvents.last
    PlayByPlayEventMessageType.valueOf(lastEvent._1.playType) == PlayByPlayEventMessageType.Make &&
      !(PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.Foul &&
        lastEvent._1.player1Id == event._1.player2Id)
  }

  def isLastEventDefensiveRebound(event: (PlayByPlayWithLineup, Option[ScoredShot]),
                                  lastEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    if (lastEvents.lengthCompare(1) > 0) {
      val lastEvent = lastEvents.last
      lastEvents.reverse.find(v => isShot(v) || isFreeThrow(v)).exists(v => isDefensiveRebound(lastEvent, v))
    } else {
      false
    }
  }

  def isSubstitutionNotDuringFreeThrows(event: (PlayByPlayWithLineup, Option[ScoredShot]),
                                        lastEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    val lastEvent = lastEvents.last
    isSubstitution(lastEvent) && !isFreeThrow(event) && !isSubstitution(event)
  }

  def isSubstitution(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.Substitution

  def isShot(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.Make ||
      PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.Miss

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

  def isPlayerDefensiveRebound(event: (PlayByPlayWithLineup, Option[ScoredShot]),
                               lastEvent: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    isRebound(event) &&
      event._1.player1TeamId != null &&
      lastEvent._1.player1TeamId != null &&
      event._1.player1TeamId != lastEvent._1.player1TeamId

  def isPlayerOffensiveRebound(event: (PlayByPlayWithLineup, Option[ScoredShot]),
                               lastEvent: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    isRebound(event) && event._1.player1TeamId == lastEvent._1.player1TeamId

  def isDefensiveRebound(event: (PlayByPlayWithLineup, Option[ScoredShot]),
                         lastEvent: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    isPlayerDefensiveRebound(event, lastEvent) ||
      isTeamDefensiveRebound(event, lastEvent)

  def isTeamRebound(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    isRebound(event) && event._1.player1TeamId == null

  def isTeamDefensiveRebound(event: (PlayByPlayWithLineup, Option[ScoredShot]),
                             lastEvent: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    isTeamRebound(event) &&
      event._1.player1Id != lastEvent._1.player1TeamId

  def isTeamOffensiveRebound(event: (PlayByPlayWithLineup, Option[ScoredShot]),
                             lastEvent: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    isTeamRebound(event) &&
      event._1.player1Id == lastEvent._1.player1TeamId

  def isLastMadeFreeThrow(lastEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    val lastEvent = lastEvents.last

    lastEvent._1.eventActionType == 16 ||
      ((lastEvent._1.eventActionType == 15 ||
        lastEvent._1.eventActionType == 12 ||
        lastEvent._1.eventActionType == 10) &&
        isMadeFreeThrow(lastEvent))
  }

  def isTechnicalFT(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    PlayByPlayEventMessageType.valueOf(event._1.playType) == PlayByPlayEventMessageType.FreeThrow &&
      event._1.eventActionType == 16


  def isTurnover(lastEvents: Seq[(PlayByPlayWithLineup, Option[ScoredShot])]): Boolean = {
    PlayByPlayEventMessageType.valueOf(lastEvents.last._1.playType) == PlayByPlayEventMessageType.Turnover
  }

  def isMadeFreeThrow(event: (PlayByPlayWithLineup, Option[ScoredShot])): Boolean =
    !Option(event._1.awayDescription).exists(_.contains("MISS")) &&
      !Option(event._1.homeDescription).exists(_.contains("MISS"))

}
