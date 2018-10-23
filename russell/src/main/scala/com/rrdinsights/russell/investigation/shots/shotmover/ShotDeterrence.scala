package com.rrdinsights.russell.investigation.shots.shotmover

import com.rrdinsights.russell.investigation.shots._
import com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedPointsArguments
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.ShotWithPlayers
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.{MapJoin, TimeUtils}

object ShotDeterrence {

  def main(strings: Array[String]): Unit = {

    val dt = TimeUtils.dtNow
    val args = ExpectedPointsArguments(strings)
    val season = args.season

    val whereSeason = s"season = '$season'"
    val whereBeforeSeason = s"season <= '$season'" // Use shots from the current season - not best practice, but don't have college shot charts for rookies

    val shots = ShotUtils.readShots(whereBeforeSeason)

    val shotChartSections = ShotUtils
      .buildShotChart(shots, dt)
      .map(v => ((v.playerId, v.bin), v))
      .toMap

    val shotChartTotalSection = ShotUtils
      .buildShotChartTotal(shots, dt)
      .map(v => (v.playerId, v))
      .toMap

    val shotsWithPlayers = ShotUtils.readShotsWithPlayers(whereSeason)

    val mappedshotsWithPlayers = shotsWithPlayers
      .map(
        v =>
          ((v.shooter,
            ShotZone
              .findShotZone(v.xCoordinate, v.yCoordinate, v.shotValue)
              .toString),
            v))

    val scoredShotSection = MapJoin
      .joinSeq(mappedshotsWithPlayers, shotChartSections)
      .map(v => (v._1.shooter, v))

    val fullScoredShots = scoredShotSection
      .map(v =>
        (v._2, buildPointsPerShotLineup(v._2._1, shotChartTotalSection)))
      .flatMap(v => toFullScoredShot(v._1._1, v._1._2, v._2, season, dt))

    val stints = reduceStints(fullScoredShots, season)

    val fullStints = addZeroBins(stints)

    writeShotStints(fullStints)
  }

  private def addZeroBins(
                           stints: Seq[ShotStintByZoneData]): Seq[ShotStintByZoneData] = {
    stints
      .groupBy(
        v =>
          (v.offensePlayer1Id,
            v.offensePlayer2Id,
            v.offensePlayer3Id,
            v.offensePlayer4Id,
            v.offensePlayer5Id,
            v.defensePlayer1Id,
            v.defensePlayer2Id,
            v.defensePlayer3Id,
            v.defensePlayer4Id,
            v.defensePlayer5Id))
      .flatMap(v => addZeroBinsToGroup(v._2))
      .toSeq
  }

  private def addZeroBinsToGroup(
                                  stints: Seq[ShotStintByZoneData]): Seq[ShotStintByZoneData] = {
    val base = stints.head
    (ShotZone.zones.map(v => emptyStintData(base, v)) ++ stints)
      .groupBy(_.bin)
      .map(v => v._2.reduce(_ + _))
      .toSeq
  }

  private def emptyStintData(base: ShotStintByZoneData,
                             bin: ShotZone): ShotStintByZoneData = {
    val newPk = Seq(
      base.offensePlayer1Id,
      base.offensePlayer2Id,
      base.offensePlayer3Id,
      base.offensePlayer4Id,
      base.offensePlayer5Id,
      base.defensePlayer1Id,
      base.defensePlayer2Id,
      base.defensePlayer3Id,
      base.defensePlayer4Id,
      base.defensePlayer5Id,
      base.season,
      bin.toString
    ).mkString("_")
    base.copy(primaryKey = newPk,
      attempts = 0,
      made = 0,
      bin = bin.toString,
      shotPoints = 0.0,
      shotExpectedPoints = 0.0,
      playerExpectedPoints = 0.0,
      difference = 0.0)
  }

  private def buildPointsPerShotLineup(
                                        shot: ShotWithPlayers,
                                        shotMap: Map[Integer, ShotChartTotalSection]): ShotChartTotalSection = {
    val attemptsAndPoitns = Seq(
      extractShotValue(shot.offensePlayer1Id, shotMap),
      extractShotValue(shot.offensePlayer2Id, shotMap),
      extractShotValue(shot.offensePlayer3Id, shotMap),
      extractShotValue(shot.offensePlayer4Id, shotMap),
      extractShotValue(shot.offensePlayer5Id, shotMap)
    ).flatten
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))

    val pointsPerShotLineup = attemptsAndPoitns._3 / attemptsAndPoitns._1
      .doubleValue()

    ShotChartTotalSection(shot.shooter,
      attemptsAndPoitns._2,
      attemptsAndPoitns._1,
      pointsPerShotLineup)
  }

  private def extractShotValue(id: Integer,
                               shotMap: Map[Integer, ShotChartTotalSection])
  : Option[(Int, Int, Double)] =
    shotMap.get(id).map(v => (v.attempts, v.made, v.attempts * v.pointsPerShot))

  private def writeShotStints(stints: Seq[ShotStintByZoneData]): Unit = {
    MySqlClient.createTable(NBATables.shot_stint_data)
    MySqlClient.insertInto(NBATables.shot_stint_data, stints)
  }

  private def reduceStints(shots: Seq[FullScoredShot],
                           season: String): Seq[ShotStintByZoneData] = {
    shots
      .groupBy(
        v =>
          (v.offensePlayer1Id,
            v.offensePlayer2Id,
            v.offensePlayer3Id,
            v.offensePlayer4Id,
            v.offensePlayer5Id,
            v.defensePlayer1Id,
            v.defensePlayer2Id,
            v.defensePlayer3Id,
            v.defensePlayer4Id,
            v.defensePlayer5Id,
            v.bin))
      .map(
        v =>
          v._2
            .map(v =>
              ShotStintByZoneData(
                Seq(
                  v.offensePlayer1Id,
                  v.offensePlayer2Id,
                  v.offensePlayer3Id,
                  v.offensePlayer4Id,
                  v.offensePlayer5Id,
                  v.defensePlayer1Id,
                  v.defensePlayer2Id,
                  v.defensePlayer3Id,
                  v.defensePlayer4Id,
                  v.defensePlayer5Id,
                  season,
                  v.bin
                ).mkString("_"),
                v.offensePlayer1Id,
                v.offensePlayer2Id,
                v.offensePlayer3Id,
                v.offensePlayer4Id,
                v.offensePlayer5Id,
                v.offenseTeamId,
                v.defensePlayer1Id,
                v.defensePlayer2Id,
                v.defensePlayer3Id,
                v.defensePlayer4Id,
                v.defensePlayer5Id,
                v.defenseTeamId,
                v.bin,
                1,
                v.shotMade,
                v.shotValue.toDouble * v.shotMade,
                v.expectedPoints,
                v.playerTotalExpectedPoints,
                v.difference,
                season
              ))
            .reduce(_ + _) / v._2.size.doubleValue())
      .toSeq
  }

  private def toFullScoredShot(
                                shotWithPlayers: ShotWithPlayers,
                                shotChart: PlayerShotChartSection,
                                playerExpectedPointsPerShot: ShotChartTotalSection,
                                season: String,
                                dt: String): Seq[FullScoredShot] = {
    val expectedPoints = (shotChart.made.toDouble / shotChart.shots.toDouble) * shotChart.value

    Seq(
      FullScoredShot(
        primaryKey =
          s"${shotWithPlayers.gameId}_${shotWithPlayers.eventNumber}_$season",
        gameId = shotWithPlayers.gameId,
        eventNumber = shotWithPlayers.eventNumber,
        shooter = shotWithPlayers.shooter,
        offenseTeamId = shotWithPlayers.offenseTeamId,
        offensePlayer1Id = shotWithPlayers.offensePlayer1Id,
        offensePlayer2Id = shotWithPlayers.offensePlayer2Id,
        offensePlayer3Id = shotWithPlayers.offensePlayer3Id,
        offensePlayer4Id = shotWithPlayers.offensePlayer4Id,
        offensePlayer5Id = shotWithPlayers.offensePlayer5Id,
        defenseTeamId = shotWithPlayers.defenseTeamId,
        defensePlayer1Id = shotWithPlayers.defensePlayer1Id,
        defensePlayer2Id = shotWithPlayers.defensePlayer2Id,
        defensePlayer3Id = shotWithPlayers.defensePlayer3Id,
        defensePlayer4Id = shotWithPlayers.defensePlayer4Id,
        defensePlayer5Id = shotWithPlayers.defensePlayer5Id,
        bin = shotChart.bin,
        shotValue = shotChart.value,
        shotAttempted = shotWithPlayers.shotAttemptedFlag,
        shotMade = shotWithPlayers.shotMadeFlag,
        expectedPoints = expectedPoints,
        playerShotAttempted = shotChart.shots,
        playerShotMade = shotChart.made,
        playerTotalExpectedPoints = playerExpectedPointsPerShot.pointsPerShot,
        difference = expectedPoints - playerExpectedPointsPerShot.pointsPerShot,
        season = season,
        dt = dt
      ),
      FullScoredShot(
        s"${shotWithPlayers.gameId}_${shotWithPlayers.eventNumber}_$season",
        shotWithPlayers.gameId,
        shotWithPlayers.eventNumber,
        shotWithPlayers.shooter,
        shotWithPlayers.offenseTeamId,
        shotWithPlayers.offensePlayer1Id,
        shotWithPlayers.offensePlayer2Id,
        shotWithPlayers.offensePlayer3Id,
        shotWithPlayers.offensePlayer4Id,
        shotWithPlayers.offensePlayer5Id,
        shotWithPlayers.defenseTeamId,
        shotWithPlayers.defensePlayer1Id,
        shotWithPlayers.defensePlayer2Id,
        shotWithPlayers.defensePlayer3Id,
        shotWithPlayers.defensePlayer4Id,
        shotWithPlayers.defensePlayer5Id,
        "Total",
        shotChart.value,
        shotWithPlayers.shotAttemptedFlag,
        shotWithPlayers.shotMadeFlag,
        expectedPoints,
        shotChart.shots,
        shotChart.made,
        playerExpectedPointsPerShot.pointsPerShot,
        expectedPoints - playerExpectedPointsPerShot.pointsPerShot,
        season,
        dt
      )
    )
  }
}
