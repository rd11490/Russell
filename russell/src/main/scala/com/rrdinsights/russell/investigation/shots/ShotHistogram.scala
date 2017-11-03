package com.rrdinsights.russell.investigation.shots

import com.rrdinsights.russell.storage.datamodel.RawShotData

object ShotHistogram {

  private val shotDistances: Map[(Int, Int), String] =
    Map(
      (0,2) -> "0-2",
      (1,2) -> "0-2",
      (2,2) -> "0-2",
      (3,2) -> "3-5",
      (4,2) -> "3-5",
      (5,2) -> "3-5",
      (6,2) -> "6-9",
      (7,2) -> "6-9",
      (8,2) -> "6-9",
      (9,2) -> "6-9",
      (10,2) -> "10-15",
      (11,2) -> "10-15",
      (12,2) -> "10-15",
      (13,2) -> "10-15",
      (14,2) -> "10-15",
      (15,2) -> "10-15",
      (16,2) -> "16-19",
      (17,2) -> "16-19",
      (18,2) -> "16-19",
      (19,2) -> "16-19",
      (20,2) -> "20-23",
      (21,2) -> "20-23",
      (22,2) -> "20-23",
      (23,2) -> "20-23",
      (22,3) -> "22-25 - 3",
      (23,3) -> "22-25 - 3",
      (24,3) -> "22-25 - 3",
      (25,3) -> "22-25 - 3",
      (26,3) -> "26+",
      (27,3) -> "26+",
      (28,3) -> "26+",
      (29,3) -> "26+",
      (30,3) -> "26+")

  def calculate(shots: Seq[RawShotData], filterBackcourt: Boolean = true): Map[ShotBinDetailed, ShotData] = {
    val detailedShots = shots
      .filter(s => s.yCoordinate != null && s.xCoordinate != null)
      .filter(s => s.shotZoneRange != "Back Court(BC)")
      .filter(s => s.shotDistance.intValue() < 30)
      .map(binAndScore)
      .groupBy(_._1)
      .map(v => reduceScoredShots(v._1, v._2))

    val overviewShots = detailedShots
      .toSeq
      .map(v => (detailedToOverview(v._1), v._2))
      .groupBy(_._1)
      .map(v => reduceScoredShotsOverview(v._1, v._2))

    detailedShots.map(v => replaceLowSample(v, overviewShots))
  }

  private def replaceLowSample(shotInfo: (ShotBinDetailed, ShotData), overviewShots: Map[ShotBinOverview, ShotData]): (ShotBinDetailed, ShotData) = {
    if (shotInfo._2.shots >= 50) {
      shotInfo
    } else {
      (shotInfo._1, overviewShots(detailedToOverview(shotInfo._1)))
    }
  }

  private def reduceScoredShots(bin: ShotBinDetailed, data: Seq[(ShotBinDetailed, ShotData)]): (ShotBinDetailed, ShotData) =
    (bin, data.map(_._2).reduce(_ + _))

  private def reduceScoredShotsOverview(bin: ShotBinOverview, data: Seq[(ShotBinOverview, ShotData)]): (ShotBinOverview, ShotData) =
    (bin, data.map(_._2).reduce(_ + _))


  private def binAndScore(shot: RawShotData): (ShotBinDetailed, ShotData) =
    (binShot(shot), scoreShot(shot))

  private def binShot(shot: RawShotData): ShotBinDetailed = {
    val shotValue = shot.shotZoneBasic.substring(0, 1).toInt
    ShotBinDetailed(shot.shotZoneRange,
      shot.shotZoneArea,
      shotDistances((shot.shotDistance.intValue(), shotValue)),
      shotValue)
  }


  private def scoreShot(shot: RawShotData): ShotData =
    ShotData(1, shot.shotMadeFlag.intValue())

  private def detailedToOverview(bin: ShotBinDetailed): ShotBinOverview =
    ShotBinOverview(bin.range, bin.area, bin.value)

}

case class ShotBinDetailed(range: String, area: String, dist: String, value: Int)

case class ShotBinOverview(range: String, area: String, value: Int)

case class ShotData(shots: Int, made: Int) {

  def +(other: ShotData): ShotData =
    ShotData(shots + other.shots, made + other.made)
}
