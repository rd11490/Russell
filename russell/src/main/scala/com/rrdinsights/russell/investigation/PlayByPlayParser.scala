package com.rrdinsights.russell.investigation

import java.{lang => jl}

import com.rrdinsights.russell.storage.datamodel.{PlayByPlayEventMessageType, PlayersOnCourt, RawPlayByPlayEvent}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

final class PlayByPlayParser(playByPlay: Seq[RawPlayByPlayEvent], playersOnCourt: Seq[PlayersOnCourt], dt: String) {

  private var currentPlayersOnCourt: PlayersOnCourtSimple = PlayByPlayParser.extractPlayersOnCourt(playersOnCourt, 1)

  def run(): Seq[PlayersOnCourt] =
    PlayByPlayParser.properlySortPlayByPlay(playByPlay)
      .map(convertPlayByPlayToCurrentPlayersOnCourt)


  def convertPlayByPlayToCurrentPlayersOnCourt(playByPlay: RawPlayByPlayEvent): PlayersOnCourt = {
    val playType = PlayByPlayEventMessageType.valueOf(playByPlay.playType)
    if (playType == PlayByPlayEventMessageType.Substitution) {
      // PLAYER1 OUT PLAYER2 IN

      if (playByPlay.player1TeamId == currentPlayersOnCourt.teamId1) {
        val players = currentPlayersOnCourt.team1Players
        val newPlayers = players.map(v => if (v == playByPlay.player1Id) playByPlay.player2Id else v)
        currentPlayersOnCourt = currentPlayersOnCourt.copy(team1Players = newPlayers)
      } else {
        val players = currentPlayersOnCourt.team2Players
        val newPlayers = players.map(v => if (v == playByPlay.player1Id) playByPlay.player2Id else v)
        currentPlayersOnCourt = currentPlayersOnCourt.copy(team2Players = newPlayers)
      }
    } else if (playType == PlayByPlayEventMessageType.StartOfPeriod) {
      currentPlayersOnCourt = PlayByPlayParser.extractPlayersOnCourt(playersOnCourt, playByPlay.eventNumber)
    }

    convertPlayersOnCourtSimpleToPlayersOnCourt(currentPlayersOnCourt, playByPlay)
  }

  private def convertPlayersOnCourtSimpleToPlayersOnCourt(playersOnCourtSimple: PlayersOnCourtSimple, playByPlay: RawPlayByPlayEvent): PlayersOnCourt = {
    val sortedTeam1Players = playersOnCourtSimple.team1Players.sorted
    val sortedTeam2Players = playersOnCourtSimple.team2Players.sorted

    PlayersOnCourt(
      s"${playByPlay.gameId}_${playByPlay.eventNumber}",
      playByPlay.gameId,
      playByPlay.eventNumber,
      playersOnCourtSimple.teamId1,
      sortedTeam1Players.head,
      sortedTeam1Players(1),
      sortedTeam1Players(2),
      sortedTeam1Players(3),
      sortedTeam1Players(4),
      playersOnCourtSimple.teamId2,
      sortedTeam2Players.head,
      sortedTeam2Players(1),
      sortedTeam2Players(2),
      sortedTeam2Players(3),
      sortedTeam2Players(4),
      dt,
      playByPlay.season)
  }

}

private[investigation] object PlayByPlayParser {
  def properlySortPlayByPlay(pbp: Seq[RawPlayByPlayEvent]): Seq[RawPlayByPlayEvent] = {
    val pbpFixed: mutable.ArrayBuffer[RawPlayByPlayEvent] = new ArrayBuffer[RawPlayByPlayEvent]()
    var i: Int = 0
    while (i < pbp.size) {
      if (i == pbp.indices.last) {
        pbpFixed.append(pbp(i))
        i+=1
      } else {
        if (PlayByPlayEventMessageType.valueOf(pbp(i).playType) == PlayByPlayEventMessageType.Substitution) {
          var j = 1
          while (i+j < pbp.size && PlayByPlayEventMessageType.valueOf(pbp(i+j).playType) == PlayByPlayEventMessageType.FreeThrow) {
            pbpFixed.append(pbp(i+j))
            j+=1
          }
          pbpFixed.append(pbp(i))
          i+=j
        } else {
          pbpFixed.append(pbp(i))
          i+=1
        }
      }
    }
    (pbpFixed zip pbp).map(v => v._1.copy(eventNumber = v._2.eventNumber))
  }

  def extractPlayersOnCourt(playersOnCourt: Seq[PlayersOnCourt], period: Int) =
    playersOnCourt
      .find(_.eventNumber == period)
      .map(v => PlayersOnCourtSimple(v))
      .getOrElse(throw new NoSuchElementException(s"No Lineup for the start of game ${playersOnCourt.head.gameId} exists"))

}

private case class PlayersOnCourtSimple(
                                   teamId1: jl.Integer,
                                   team1Players: Seq[jl.Integer],
                                   teamId2: jl.Integer,
                                   team2Players: Seq[jl.Integer])

private object PlayersOnCourtSimple {
  def apply(playersOnCourt: PlayersOnCourt): PlayersOnCourtSimple =
    PlayersOnCourtSimple(
      playersOnCourt.teamId1,
      Seq(
        playersOnCourt.team1player1Id,
        playersOnCourt.team1player2Id,
        playersOnCourt.team1player3Id,
        playersOnCourt.team1player4Id,
        playersOnCourt.team1player5Id),
      playersOnCourt.teamId2,
      Seq(
        playersOnCourt.team2player1Id,
        playersOnCourt.team2player2Id,
        playersOnCourt.team2player3Id,
        playersOnCourt.team2player4Id,
        playersOnCourt.team2player5Id))
}