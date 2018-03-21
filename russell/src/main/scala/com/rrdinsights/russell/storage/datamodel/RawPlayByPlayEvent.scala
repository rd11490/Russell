package com.rrdinsights.russell.storage.datamodel

import java.sql.ResultSet
import java.{lang => jl}

import com.rrdinsights.russell.utils.TimeUtils
import com.rrdinsights.scalabrine.models.PlayByPlayEvent

final case class RawPlayByPlayEvent(
                                     primaryKey: String,
                                     gameId: String,
                                     eventNumber: jl.Integer,
                                     playType: String,
                                     eventActionType: jl.Integer, // TODO - convert to case objects
                                     period: jl.Integer,
                                     wcTimeString: String, // TODO - find out what this means
                                     pcTimeString: String,
                                     homeDescription: String,
                                     neutralDescription: String,
                                     awayDescription: String,

                                     homeScore: jl.Integer,
                                     awayScore: jl.Integer,

                                     player1Type: jl.Integer, // TODO - find out what this means
                                     player1Id: jl.Integer,
                                     player1Name: String,
                                     player1TeamId: jl.Integer,
                                     player1TeamCity: String,
                                     player1TeamNickname: String,
                                     player1TeamAbbreviation: String,

                                     player2Type: jl.Integer, // TODO - find out what this means
                                     player2Id: jl.Integer,
                                     player2Name: String,
                                     player2TeamId: jl.Integer,
                                     player2TeamCity: String,
                                     player2TeamNickname: String,
                                     player2TeamAbbreviation: String,

                                     player3Type: jl.Integer, // TODO - find out what this means
                                     player3Id: jl.Integer,
                                     player3Name: String,
                                     player3TeamId: jl.Integer,
                                     player3TeamCity: String,
                                     player3TeamNickname: String,
                                     player3TeamAbbreviation: String,

                                     timeElapsed: jl.Integer,
                                     season: String,
                                     dt: String) extends Ordered[RawPlayByPlayEvent] {

  override def compare(that: RawPlayByPlayEvent): Int =
    sortByTimeLeft(that) match {
      case 0 => sortByEventNumber(that)
      case n => n
    }

  private def sortByTimeLeft(that: RawPlayByPlayEvent): Int =
    this.timeElapsed.compareTo(that.timeElapsed)

  private def sortByEventNumber(that: RawPlayByPlayEvent): Int =
    this.eventNumber.compareTo(that.eventNumber)
}

object RawPlayByPlayEvent extends ResultSetMapper {

  def apply(playByPlayEvent: PlayByPlayEvent, season: String, dt: String): RawPlayByPlayEvent =
    RawPlayByPlayEvent(
      s"${playByPlayEvent.gameId}_${playByPlayEvent.eventNumber}",
      playByPlayEvent.gameId,
      playByPlayEvent.eventNumber,
      PlayByPlayEventMessageType(playByPlayEvent.eventMessageType).toString,
      playByPlayEvent.eventActionType,
      playByPlayEvent.period,
      playByPlayEvent.wcTimeString,
      playByPlayEvent.pcTimeString,
      playByPlayEvent.homeDescription,
      playByPlayEvent.neutralDescription,
      playByPlayEvent.awayDescription,
      playByPlayEvent.homeScore,
      playByPlayEvent.awayScore,
      playByPlayEvent.player1Type,
      playByPlayEvent.player1Id,
      playByPlayEvent.player1Name,
      playByPlayEvent.player1TeamId,
      playByPlayEvent.player1TeamCity,
      playByPlayEvent.player1TeamNickname,
      playByPlayEvent.player1TeamAbbreviation,
      playByPlayEvent.player2Type,
      playByPlayEvent.player2Id,
      playByPlayEvent.player2Name,
      playByPlayEvent.player2TeamId,
      playByPlayEvent.player2TeamCity,
      playByPlayEvent.player2TeamNickname,
      playByPlayEvent.player2TeamAbbreviation,
      playByPlayEvent.player3Type,
      playByPlayEvent.player3Id,
      playByPlayEvent.player3Name,
      playByPlayEvent.player3TeamId,
      playByPlayEvent.player3TeamCity,
      playByPlayEvent.player3TeamNickname,
      playByPlayEvent.player3TeamAbbreviation,
      TimeUtils.convertTimeStringToTime(playByPlayEvent.period, playByPlayEvent.pcTimeString),
      season,
      dt)

  def apply(resultSet: ResultSet): RawPlayByPlayEvent =
    RawPlayByPlayEvent(
      getString(resultSet, 0),
      getString(resultSet, 1),
      getInt(resultSet, 2),
      getString(resultSet, 3),
      getInt(resultSet, 4),
      getInt(resultSet, 5),
      getString(resultSet, 6),
      getString(resultSet, 7),
      getString(resultSet, 8),
      getString(resultSet, 9),
      getString(resultSet, 10),

      getInt(resultSet, 11),
      getInt(resultSet, 12),

      getInt(resultSet, 13),
      getInt(resultSet, 14),
      getString(resultSet, 15),
      getInt(resultSet, 16),
      getString(resultSet, 17),
      getString(resultSet, 18),
      getString(resultSet, 19),

      getInt(resultSet, 20),
      getInt(resultSet, 21),
      getString(resultSet, 22),
      getInt(resultSet, 23),
      getString(resultSet, 24),
      getString(resultSet, 25),
      getString(resultSet, 26),

      getInt(resultSet, 27),
      getInt(resultSet, 28),
      getString(resultSet, 29),
      getInt(resultSet, 30),
      getString(resultSet, 31),
      getString(resultSet, 32),
      getString(resultSet, 33),

      getInt(resultSet, 34),
      getString(resultSet, 35),
      getString(resultSet, 36))
}

sealed trait PlayByPlayEventMessageType {
  def value: jl.Integer
}

object PlayByPlayEventMessageType {

  private val messages: Seq[PlayByPlayEventMessageType] = Seq(
    Make, Miss, FreeThrow, Rebound, Turnover, Foul, Violation,
    Substitution, Timeout, JumpBall, Ejection, StartOfPeriod,
    EndOfPeriod, Empty)

  def apply(i: jl.Integer): PlayByPlayEventMessageType =
    messages.find(_.value == i).getOrElse(Empty)

  def valueOf(str: String): PlayByPlayEventMessageType =
    messages.find(_.toString == str)
    .getOrElse(throw new IllegalArgumentException(s"$str is not a valid event type"))


  case object Make extends PlayByPlayEventMessageType {
    override def value: Integer = 1
  }

  case object Miss extends PlayByPlayEventMessageType {
    override def value: Integer = 2
  }

  case object FreeThrow extends PlayByPlayEventMessageType {
    override def value: Integer = 3
  }

  case object Rebound extends PlayByPlayEventMessageType {
    override def value: Integer = 4
  }

  case object Turnover extends PlayByPlayEventMessageType {
    override def value: Integer = 5
  }

  case object Foul extends PlayByPlayEventMessageType {
    override def value: Integer = 6
  }

  case object Violation extends PlayByPlayEventMessageType {
    override def value: Integer = 7
  }

  case object Substitution extends PlayByPlayEventMessageType {
    override def value: Integer = 8
  }

  case object Timeout extends PlayByPlayEventMessageType {
    override def value: Integer = 9
  }

  case object JumpBall extends PlayByPlayEventMessageType {
    override def value: Integer = 10
  }

  case object Ejection extends PlayByPlayEventMessageType {
    override def value: Integer = 11
  }

  case object StartOfPeriod extends PlayByPlayEventMessageType {
    override def value: Integer = 12
  }

  case object EndOfPeriod extends PlayByPlayEventMessageType {
    override def value: Integer = 13
  }

  case object Empty extends PlayByPlayEventMessageType {
    override def value: Integer = 18
  }

}
