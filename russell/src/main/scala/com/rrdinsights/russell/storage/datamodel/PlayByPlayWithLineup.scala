package com.rrdinsights.russell.storage.datamodel

import java.sql.ResultSet
import java.{lang => jl}

final case class PlayByPlayWithLineup(primaryKey: String,
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
                                      player1TeamId: jl.Integer,

                                      player2Type: jl.Integer, // TODO - find out what this means
                                      player2Id: jl.Integer,
                                      player2TeamId: jl.Integer,

                                      player3Type: jl.Integer, // TODO - find out what this means
                                      player3Id: jl.Integer,
                                      player3TeamId: jl.Integer,

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

                                      timeElapsed: jl.Integer,
                                      season: String,
                                      seasonType: String,
                                      dt: String) extends Ordered[PlayByPlayWithLineup]{
  override def compare(that: PlayByPlayWithLineup): Int =
    sortByTimeLeft(that) match {
      case 0 => sortByEventNumber(that)
      case n => n
    }

  private def sortByTimeLeft(that: PlayByPlayWithLineup): Int =
    this.timeElapsed.compareTo(that.timeElapsed)

  private def sortByEventNumber(that: PlayByPlayWithLineup): Int =
    this.eventNumber.compareTo(that.eventNumber)

}

object PlayByPlayWithLineup extends ResultSetMapper {

  def apply(playByPlay: RawPlayByPlayEvent, playersOnCourt: PlayersOnCourt, dt: String): PlayByPlayWithLineup =
    PlayByPlayWithLineup(
      playByPlay.primaryKey,
      playByPlay.gameId,
      playByPlay.eventNumber,
      playByPlay.playType,
      playByPlay.eventActionType, // TODO - convert to case objects
      playByPlay.period,
      playByPlay.wcTimeString, // TODO - find out what this means
      playByPlay.pcTimeString,
      playByPlay.homeDescription,
      playByPlay.neutralDescription,
      playByPlay.awayDescription,

      playByPlay.homeScore,
      playByPlay.awayScore,

      playByPlay.player1Type, // TODO - find out what this means
      playByPlay.player1Id,
      playByPlay. player1TeamId,

      playByPlay.player2Type, // TODO - find out what this means
      playByPlay.player2Id,
      playByPlay.player2TeamId,

      playByPlay.player3Type, // TODO - find out what this means
      playByPlay.player3Id,
      playByPlay.player3TeamId,

      playersOnCourt.teamId1,
      playersOnCourt.team1player1Id,
      playersOnCourt.team1player2Id,
      playersOnCourt.team1player3Id,
      playersOnCourt.team1player4Id,
      playersOnCourt.team1player5Id,
      playersOnCourt.teamId2,
      playersOnCourt.team2player1Id,
      playersOnCourt.team2player2Id,
      playersOnCourt.team2player3Id,
      playersOnCourt.team2player4Id,
      playersOnCourt.team2player5Id,

      playByPlay.timeElapsed,
      playByPlay.season,
      playByPlay.seasonType,
      dt)

  def apply(resultSet: ResultSet): PlayByPlayWithLineup =
    PlayByPlayWithLineup(
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
      getInt(resultSet, 15),
      getInt(resultSet, 16),
      getInt(resultSet, 17),
      getInt(resultSet, 18),
      getInt(resultSet, 19),
      getInt(resultSet, 20),
      getInt(resultSet, 21),
      getInt(resultSet, 22),
      getInt(resultSet, 23),
      getInt(resultSet, 24),
      getInt(resultSet, 25),
      getInt(resultSet, 26),
      getInt(resultSet, 27),
      getInt(resultSet, 28),
      getInt(resultSet, 29),
      getInt(resultSet, 30),
      getInt(resultSet, 31),
      getInt(resultSet, 32),
      getInt(resultSet, 33),
      getInt(resultSet, 34),

      getString(resultSet, 35),
      getString(resultSet, 36),
      getString(resultSet, 36))
}
