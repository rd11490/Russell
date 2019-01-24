package com.rrdinsights.russell.investigation.playbyplay.luckadjusted

import java.{lang => jl}

import com.rrdinsights.russell.investigation.playbyplay.luckadjusted.LuckAdjustedStints.writeSecondsPlayed
import com.rrdinsights.russell.investigation.shots.ShotUtils
import com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedPointsArguments
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel._
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.{MapJoin, TimeUtils}

object LuckAdjustedStintsMultiYear {

  /**
    * This object is for calculating stints for luck adjusted RAPM
    *
    */
  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow
    val args = ExpectedPointsArguments(strings)
    val seasons = args.seasons
    val season = s"${seasons.head.split("-")(0)}-${seasons.last.split("-")(1)}"
    val seasonType = args.seasonType
    val whereSeason = seasons.map(season => s"season = '$season'").mkString(" OR ")
    val whereSeasonType = s"seasontype = '$seasonType'"

    println(whereSeason)
    val playByPlay = LuckAdjustedUtils.readPlayByPlay(whereSeason, whereSeasonType)


    val freeThrowMap = LuckAdjustedUtils.buildPlayerCareerFreeThrowPercentMap()

    val seasonShots = ShotUtils.readScoredShots(whereSeason)

    val keyedPbP = playByPlay.map(v => ((v.gameId, v.eventNumber), v)).toMap
    val keyedShots = seasonShots.map(v => ((v.gameId, v.eventNumber), v)).toMap

    val playByPlayWithShotData = MapJoin.leftOuterJoin(keyedPbP, keyedShots)

    val stints = playByPlayWithShotData
      .groupBy(v => (v._1.gameId, v._1.period))
      .flatMap(v => {
        val sorted = v._2.sortBy(_._1)
        LuckAdjustedUtils
          .seperatePossessions(sorted)
          .map(c => parsePossesions(c, sorted, freeThrowMap, dt))
      })
      .groupBy(_.primaryKey)
      .map(v => v._2.reduce(_ + _))
      .toSeq


    val multiSeasonStints = stints.map(v => v.copy(season = season))

    writeStints(multiSeasonStints)

    val oneWayStints = multiSeasonStints
      .flatMap(_.toOneWayStints)
      .groupBy(_.primaryKey)
      .map(v => v._2.reduce(_ + _))
      .toSeq

    writeOneWayStints(oneWayStints)

    val secondsPlayed = stints
      .flatMap(v => {
        Seq(
          v.team1player1Id,
          v.team1player2Id,
          v.team1player3Id,
          v.team1player4Id,
          v.team1player5Id
        ).map(i =>
          SecondsPlayedContainer(s"${i}_${v.season}", i, v.seconds, v.team1Possessions, v.team2Possessions, v.season, v.seasonType)) ++
          Seq(
            v.team2player1Id,
            v.team2player2Id,
            v.team2player3Id,
            v.team2player4Id,
            v.team2player5Id).map(i =>
            SecondsPlayedContainer(s"${i}_${v.season}", i, v.seconds, v.team2Possessions, v.team1Possessions, v.season, v.seasonType))
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

  private def writeSecondsPlayed(
                                  secondsPlayed: Seq[SecondsPlayedContainer]): Unit = {
    MySqlClient.createTable(NBATables.seconds_played)
    MySqlClient.insertInto(NBATables.seconds_played, secondsPlayed)
  }

  private def parsePossesions(eventsWithTime: (Seq[(PlayByPlayWithLineup,
    Option[ScoredShot])],
    Integer),
                              allEvents: Seq[(PlayByPlayWithLineup,
                                Option[ScoredShot])],
                              freeThrowMap: Map[jl.Integer, jl.Double],
                              dt: String): LuckAdjustedStint = {

    val events = eventsWithTime._1
    val time = eventsWithTime._2
    val points = LuckAdjustedUtils.countPoints(events, freeThrowMap)
    val turnovers = LuckAdjustedUtils.countTurnovers(events)
    val rebounds = LuckAdjustedUtils.countRebounds(events, allEvents)

    val first = events.head
    val team1Points = points.find(_.teamId == first._1.teamId1)
    val team2Points = points.find(_.teamId == first._1.teamId2)
    val team1Turnovers =
      turnovers.find(_.teamId == first._1.teamId1).map(_.turnovers)
    val team2Turnovers =
      turnovers.find(_.teamId == first._1.teamId2).map(_.turnovers)
    val team1Rebounds = rebounds.find(_.teamId == first._1.teamId1)
    val team2Rebounds = rebounds.find(_.teamId == first._1.teamId2)

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
      first._1.season
    ).mkString("_")

    val possession = LuckAdjustedUtils.determinePossession(events)
    val team1Possessions = if (possession == first._1.teamId1) 1 else 0
    val team2Possessions = if (possession == first._1.teamId2) 1 else 0

    LuckAdjustedStint(
      primaryKey = primaryKey,
      season = first._1.season,
      seasonType = first._1.seasonType,
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
      team1DefensiveRebounds =
        team1Rebounds.map(_.defensiveRebounds).getOrElse(0),
      team2DefensiveRebounds =
        team2Rebounds.map(_.defensiveRebounds).getOrElse(0),
      team1OffensiveRebounds =
        team1Rebounds.map(_.offensiveRebounds).getOrElse(0),
      team2OffensiveRebounds =
        team2Rebounds.map(_.offensiveRebounds).getOrElse(0),
      team1Turnovers = team1Turnovers.getOrElse(0),
      team2Turnovers = team2Turnovers.getOrElse(0),

      team1FieldGoalAttempts = team1Points.map(_.fieldGoalAttempts).getOrElse(0),
      team1FieldGoals = team1Points.map(_.fieldGoals).getOrElse(0),
      team1ThreePtAttempts = team1Points.map(_.threePtAttempts).getOrElse(0),
      team1ThreePtMade = team1Points.map(_.threePtMade).getOrElse(0),
      team1FreeThrowAttempts = team1Points.map(_.freeThrowAttempts).getOrElse(0),
      team1FreeThrowsMade = team1Points.map(_.freeThrowsMade).getOrElse(0),

      team2FieldGoalAttempts = team2Points.map(_.fieldGoalAttempts).getOrElse(0),
      team2FieldGoals = team2Points.map(_.fieldGoals).getOrElse(0),
      team2ThreePtAttempts = team2Points.map(_.threePtAttempts).getOrElse(0),
      team2ThreePtMade = team2Points.map(_.threePtMade).getOrElse(0),
      team2FreeThrowAttempts = team2Points.map(_.freeThrowAttempts).getOrElse(0),
      team2FreeThrowsMade = team2Points.map(_.freeThrowsMade).getOrElse(0),

      team1Possessions = team1Possessions,
      team2Possessions = team2Possessions,
      seconds = time
    )
  }
}

