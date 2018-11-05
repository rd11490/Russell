package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{
  DataModelUtils,
  PlayersOnCourt,
  RawPlayByPlayEvent
}
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.TimeUtils
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.AdvancedBoxScoreEndpoint
import com.rrdinsights.scalabrine.models.PlayerStats
import com.rrdinsights.scalabrine.parameters._

object PlayersOnCourtDownloader {

  def downloadPlayersOnCourtAtEvent(playByPlay: RawPlayByPlayEvent,
                                    dt: String): Option[PlayersOnCourt] = {
    val time = TimeUtils.convertTimeStringToTime(playByPlay.period.intValue,
                                                 playByPlay.pcTimeString) * 10
    val start = if (time < 700) 0 else time - 9
    val end = time + 9
    val players = PlayersOnCourtDownloader.downloadPlayersOnCourt(
      playByPlay.gameId,
      playByPlay.season,
      playByPlay.seasonType,
      start,
      end)

    val timePlayed = players
      .map(v => v._3.minutes)
      .groupBy(v => v)
      .map(v => (v._1, v._2.size))
      .maxBy(_._2)
      ._1

    val playersSelected = players.filter(_._3.minutes == timePlayed)

    if (playersSelected.size == 10 && playersSelected
          .slice(0, 4)
          .map(_._2)
          .distinct
          .size == 1 && playersSelected
          .slice(5, 9)
          .map(_._2)
          .distinct
          .size == 1) {
      val primaryKey = s"${playByPlay.gameId}_${playByPlay.period}"
      Some(
        PlayersOnCourt(
          primaryKey,
          playByPlay.gameId,
          null,
          playByPlay.period,
          players.head._2,
          players.head._1,
          players(1)._1,
          players(2)._1,
          players(3)._1,
          players(4)._1,
          players(5)._2,
          players(5)._1,
          players(6)._1,
          players(7)._1,
          players(8)._1,
          players(9)._1,
          dt,
          DataModelUtils.gameIdToSeason(playByPlay.gameId),
          playByPlay.seasonType
        ))

    } else {
      println(s"FAILURE")
      println(s"${playByPlay.gameId}-${playByPlay.period}")
      println(s"$time")
      println(s"${players.size}")
      println(
        s"${players.groupBy(_._2).map(v => s"${v._1} - ${v._2.size}").mkString(" | ")}")
      None
    }
  }

  def playersToIgnoreCalc(
      subsIn: Seq[(Integer, (Integer, String))],
      subsOut: Seq[(Integer, (Integer, String))]): Seq[Integer] = {
    (subsIn ++ subsOut)
      .groupBy(_._1)
      .map(v => (v._1, v._2.sortBy(_._2._1).map(_._2._2)))
      .filter(_._2.head.equalsIgnoreCase("IN"))
      .keys
      .toSeq
  }

  def downloadPlayersOnCourtAtEvent2(
      playByPlay: (RawPlayByPlayEvent, Seq[RawPlayByPlayEvent]),
      dt: String): Option[PlayersOnCourt] = {
    val referencePlay = playByPlay._1
    val period = referencePlay.period

    val time = TimeUtils.timeFromStartOfGameAtPeriod(period) * 10
    val start = time + 10
    val end = time + 180 * 10
    val players = PlayersOnCourtDownloader.downloadPlayersOnCourt(
      referencePlay.gameId,
      referencePlay.season,
      referencePlay.seasonType,
      start,
      end)

    val subs = playByPlay._2
      .map(v => (v.eventNumber, v.player1Id, v.player2Id))
      .sortBy(_._1)

    val subsIn = subs.map(v => (v._3, (v._1, "IN")))
    val subsOut = subs.map(v => (v._2, (v._1, "OUT")))

    val playersToIgnore = playersToIgnoreCalc(subsIn, subsOut)

//    val subIn = playByPlay._2.map(v => v.player2Id).map(v => (v, v)).groupBy(_._1).map(v => (v._1, v._2.length))
//    val subOut = playByPlay._2.map(v => v.player1Id).map(v => (v, v)).groupBy(_._1).map(v => (v._1, v._2.length))
//
//    val subedInAndOut = subIn.filter(v => subOut.exists(c => v._1 == c._1 && c._2 <= v._2))
//
//    val fixedSubIn = subIn.filterNot(v => subedInAndOut.contains(v))
//    val fixedSubOut = subOut.filterNot(v => subedInAndOut.contains(v))
//
    val startingPlayers = players
      .filterNot(v => playersToIgnore.contains(v._1))
      .sortBy(v => (v._2, v._1))

    if (startingPlayers.size == 10 && startingPlayers
          .slice(0, 4)
          .map(_._2)
          .distinct
          .size == 1 && startingPlayers
          .slice(5, 9)
          .map(_._2)
          .distinct
          .size == 1) {
      val primaryKey = s"${referencePlay.gameId}_${referencePlay.period}"
      Some(
        PlayersOnCourt(
          primaryKey,
          referencePlay.gameId,
          null,
          referencePlay.period,
          startingPlayers.head._2,
          startingPlayers.head._1,
          startingPlayers(1)._1,
          startingPlayers(2)._1,
          startingPlayers(3)._1,
          startingPlayers(4)._1,
          startingPlayers(5)._2,
          startingPlayers(5)._1,
          startingPlayers(6)._1,
          startingPlayers(7)._1,
          startingPlayers(8)._1,
          startingPlayers(9)._1,
          dt,
          referencePlay.season,
          referencePlay.seasonType
        ))

    } else {
      println(s"FAILURE")
      println(s"${referencePlay.gameId}-${referencePlay.period}")
      println(s"$time")
      println(s"${players.size}")
      println(
        s"${players.groupBy(_._2).map(v => s"${v._1} - ${v._2.size}").mkString(" | ")}")

      println(s"Substitution in first minutes Players: ")
      println("All Players")
      players.foreach(println)
      println("Starting Players")
      startingPlayers.foreach(println)

      println("PBP")
      playByPlay._2.foreach(println)
      println("Subs")
      subs.foreach(println)
      println("Subs In")
      subsIn.foreach(println)
      println("Subs Out")
      subsOut.foreach(println)
      println("Players To Filter Out")
      playersToIgnore.foreach(println)
      //None
      throw new Exception("FAILURE!")
    }
  }

  def downloadPlayersOnCourt(
      gameId: String,
      season: String,
      seasonType: String,
      timeStart: Int,
      timeEnd: Int): Seq[(Integer, Integer, PlayerStats)] = {
    val gameIdParameter = GameIdParameter.newParameterValue(gameId)
    val rangeType = RangeTypeParameter.newParameterValue("2")
    val startRange = StartRangeParameter.newParameterValue(timeStart.toString)
    val endRange = EndRangeParameter.newParameterValue(timeEnd.toString)
    val seasonParam = SeasonParameter.newParameterValue(season)
    val seasonTypeParam = SeasonTypeParameter.newParameterValue(seasonType)

    val endpoint = AdvancedBoxScoreEndpoint(gameId = gameIdParameter,
                                            season = seasonParam,
                                            seasonType = seasonTypeParam,
                                            rangeType = rangeType,
                                            startRange = startRange,
                                            endRange = endRange)

    println(endpoint.url)

    Thread.sleep(500)
    ScalabrineClient
      .getAdvancedBoxScore(endpoint)
      .boxScoreAdvanced
      .playerStats
      .map(v => (v.playerId, v.teamId, v))
      .sortBy(v => (v._2, v._1))
  }

  def writePlayersOnCourt(players: Seq[PlayersOnCourt]): Unit = {
    MySqlClient.createTable(NBATables.players_on_court)
    MySqlClient.insertInto(NBATables.players_on_court, players)
  }

  def writePlayersOnCourtAtPeriod(players: Seq[PlayersOnCourt]): Unit = {
    MySqlClient.createTable(NBATables.players_on_court_at_period)
    MySqlClient.insertInto(NBATables.players_on_court_at_period, players)
  }

  def readPlayersOnCourtAtPeriod(where: String*): Seq[PlayersOnCourt] = {
    MySqlClient.selectFrom[PlayersOnCourt](NBATables.players_on_court_at_period,
                                           PlayersOnCourt.apply,
                                           where: _*)
  }

  def readPlayersOnCourt(where: String*): Seq[PlayersOnCourt] = {
    MySqlClient.selectFrom[PlayersOnCourt](NBATables.players_on_court,
                                           PlayersOnCourt.apply,
                                           where: _*)
  }
}
