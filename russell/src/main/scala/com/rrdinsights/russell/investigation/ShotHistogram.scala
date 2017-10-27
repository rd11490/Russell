package com.rrdinsights.russell.investigation

import com.rrdinsights.russell.storage.datamodel.RawShotData

object ShotHistogram {

  def calculate(shots: Seq[RawShotData], binWidth: Int = 50, binHeight: Int = 50): Map[ShotBin, ShotData] = {
    shots.filter(s => s.yCoordinate != null && s.xCoordinate != null)
      .map(binAndScore(_, binWidth, binHeight))
      .groupBy(_._1)
      .map(v => reduceScoredShots(v._1, v._2))
  }

  private def reduceScoredShots(bin: ShotBin, data: Seq[(ShotBin, ShotData)]): (ShotBin, ShotData) =
    (bin, data.map(_._2).reduce(_ + _))


  private def binAndScore(shot: RawShotData, binWidth: Int, binHeight: Int): (ShotBin, ShotData) =
    (binShot(shot, binWidth, binHeight), scoreShot(shot))

  private def binShot(shot: RawShotData, binWidth: Int, binHeight: Int): ShotBin =
    ShotBin(
      shot.xCoordinate.intValue() / binWidth,
      shot.yCoordinate.intValue() / binHeight,
      shot.shotType.charAt(0).toInt)

  private def scoreShot(shot: RawShotData): ShotData =
    ShotData(1, shot.shotMadeFlag.intValue())

}

case class ShotBin(xBni: Int, yBin: Int, value: Int) //X, Y, 2 or 3
case class ShotData(shots: Int, made: Int) {

  def +(other: ShotData): ShotData =
    ShotData(shots + other.shots, made + other.made)
}
