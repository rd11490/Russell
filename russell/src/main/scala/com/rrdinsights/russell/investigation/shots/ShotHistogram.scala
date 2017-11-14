package com.rrdinsights.russell.investigation.shots

import java.sql.ResultSet
import java.{lang => jl}

import com.rrdinsights.russell.storage.datamodel.{RawShotData, ResultSetMapper}

/**
  * Shot Chart areas:
  *   RestrictedLeft: X < 0 and dist < 4ft
  *   RestrictedRight: X > 0 and dist < 4ft
  *
  *   LeftCorner3: Y < 92.5, value = 3, X < 0
  *   RightCorner3: Y < 92.5, value = 3, X > 0
  *
  *
  *
  */
object ShotHistogram {

  // TODO Rebuild binning to go to shot zones based on python code


  def calculate(shots: Seq[RawShotData], filterBackcourt: Boolean = true): Map[ShotBinDetailed, ShotData] = {
    val detailedShots = shots
      .filter(s => s.yCoordinate != null && s.xCoordinate != null)
      .filter(s => s.shotZoneRange != "Back Court(BC)")
      .filter(s => s.shotDistance.intValue() < 30)
      .map(binAndScore)
      .groupBy(_._1)
      .map(v => reduceScoredShots(v._1, v._2))

    detailedShots

  }

  /*
  // TODO Look into if this is useful
  private def replaceLowSample(shotInfo: (ShotBinDetailed, ShotData), overviewShots: Map[ShotBinOverview, ShotData]): (ShotBinDetailed, ShotData) = {
    if (shotInfo._2.shots >= 50) {
      shotInfo
    } else {
      (shotInfo._1, overviewShots(detailedToOverview(shotInfo._1)))
    }
  }
  */

  private def reduceScoredShots(bin: ShotBinDetailed, data: Seq[(ShotBinDetailed, ShotData)]): (ShotBinDetailed, ShotData) =
    (bin, data.map(_._2).reduce(_ + _))

  private def reduceScoredShotsOverview(bin: ShotBinOverview, data: Seq[(ShotBinOverview, ShotData)]): (ShotBinOverview, ShotData) =
    (bin, data.map(_._2).reduce(_ + _))


  def binAndScore(shot: RawShotData): (ShotBinDetailed, ShotData) =
    (binShot(shot), scoreShot(shot))

  private def binShot(shot: RawShotData): ShotBinDetailed = {
    ShotBinDetailed(
      shot.playerId,
      chooseBin(shot),
      shot.shotValue)
  }

  private def scoreShot(shot: RawShotData): ShotData =
    ShotData(1, shot.shotMadeFlag.intValue())


  private def chooseBin(shot: RawShotData): String =
    ShotZone.findShotZone(shot).toString

}

case class ShotBinDetailed(playerId: jl.Integer, bin: String, value: Int)

case class ShotBinOverview(range: String, area: String, value: Int)

case class ShotData(shots: Int, made: Int) {

  def +(other: ShotData): ShotData =
    ShotData(shots + other.shots, made + other.made)
}

case class PlayerShotChartSection(primaryKey: String, playerId: jl.Integer, bin: String, value: Int, shots: Int, made: Int, dt: String)

object PlayerShotChartSection extends ResultSetMapper {

  def apply(bin: ShotBinDetailed, data: ShotData, dt: String): PlayerShotChartSection =
    PlayerShotChartSection(
      s"${bin.playerId}_${bin.bin}",
      bin.playerId,
      bin.bin,
      bin.value,
      data.shots,
      data.made,
      dt)

  def apply(resultSet: ResultSet): PlayerShotChartSection =
    PlayerShotChartSection(
      getString(resultSet, 0),
      getInt(resultSet, 1),
      getString(resultSet, 2),
      getInt(resultSet, 3),
      getInt(resultSet, 4),
      getInt(resultSet, 5),
      getString(resultSet, 6)
    )
}