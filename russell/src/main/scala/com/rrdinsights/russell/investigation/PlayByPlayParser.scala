package com.rrdinsights.russell.investigation

import java.{lang => jl}

import com.rrdinsights.russell.storage.datamodel.{PlayByPlayEventMessageType, PlayersOnCourt, RawPlayByPlayEvent}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

final class PlayByPlayParser(playByPlay: Seq[RawPlayByPlayEvent], playersOnCourt: Seq[PlayersOnCourt]) {

  private var currentPlayersOnCourt: PlayersOnCourtSimple = playersOnCourt
    .find(_.eventNumber == 1)
    .map(v => PlayersOnCourtSimple(v))
    .getOrElse(throw new NoSuchElementException(s"No Lineup for the start of game ${playByPlay.head.gameId} exists"))


  def run(): Seq[PlayersOnCourt] = {
    PlayByPlayParser.properlySortPlayByPlay(playByPlay)
      .map()

    playersOnCourt
  }



  def convertPlayByPlayToCurrentPlayersOnCourt(playByPlay: RawPlayByPlayEvent): PlayersOnCourt = {

    
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