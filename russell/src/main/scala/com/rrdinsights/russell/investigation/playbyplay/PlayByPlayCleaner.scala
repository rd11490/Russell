package com.rrdinsights.russell.investigation.playbyplay

import com.rrdinsights.russell.investigation.playbyplay.luckadjusted.LuckAdjustedUtils
import com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedPointsArguments
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{PlayByPlayWithLineup}
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.TimeUtils

object PlayByPlayCleaner {

  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow
    val args = ExpectedPointsArguments(strings)
    val season = args.season
    val whereSeason = s"season = '$season'"

    val playByPlay = LuckAdjustedUtils.readPlayByPlay(whereSeason)
    val fixed = playByPlay.groupBy(v => (v.timeElapsed, v.gameId, v.period))
      .filter(v => containsReboundAndShot(v._2))
      .filter(v => rebounderIsNotShooter(v._2))
      .flatMap(v => flipReboundAndShotEvent(v._2))
      .toSeq

    writePlayByPlay(fixed)
  }

  private def containsReboundAndShot(plays: Seq[PlayByPlayWithLineup]): Boolean =
    plays.exists(v => PlayByPlayUtils.isMiss((v, None)) || PlayByPlayUtils.isMissedFT((v, None))) &&
    plays.exists(v => PlayByPlayUtils.isRebound((v, None)))

  private def rebounderIsNotShooter(plays: Seq[PlayByPlayWithLineup]): Boolean = {
    val rebound = plays.find(v => PlayByPlayUtils.isRebound((v, None))).get
    val shot = plays.find(v => PlayByPlayUtils.isMiss((v, None)) || PlayByPlayUtils.isMissedFT((v, None))).get

    rebound.player1Id != shot.player1Id
  }

  private def flipReboundAndShotEvent(plays: Seq[PlayByPlayWithLineup]): Seq[PlayByPlayWithLineup] = {
    val rebound = plays.find(v => PlayByPlayUtils.isRebound((v, None))).get
    val shot = plays.find(v => PlayByPlayUtils.isMiss((v, None)) || PlayByPlayUtils.isMissedFT((v, None))).get

    if (rebound.eventNumber < shot.eventNumber) {
      val reboundNum = rebound.eventNumber
      val shotNum = shot.eventNumber

      Seq(
        rebound.copy(primaryKey = s"${rebound.gameId}_$shotNum", eventNumber = shotNum),
        shot.copy(primaryKey = s"${shot.gameId}_$reboundNum", eventNumber = reboundNum))
    } else {
      Seq.empty
    }
  }

  private def writePlayByPlay(pbp: Seq[PlayByPlayWithLineup]): Unit = {
    MySqlClient.createTable(NBATables.play_by_play_with_lineup)
    MySqlClient.insertInto(NBATables.play_by_play_with_lineup, pbp)
  }

}
