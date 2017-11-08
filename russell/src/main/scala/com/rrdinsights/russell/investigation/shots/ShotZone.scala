package com.rrdinsights.russell.investigation.shots

import com.rrdinsights.russell.storage.datamodel.RawShotData

sealed trait ShotZone {
  def isInZone(shot: RawShotData): Boolean

  def value: Int
}

sealed trait AboveCornerShotZone extends ShotZone {
  override def isInZone(shot: RawShotData): Boolean = {
    val theta = ShotZone.theta(shot)
    val shotValue = ShotZone.shotValue(shot)
    val dist = ShotZone.distance(shot)
    shotValue == value &&
      theta >= thetaMin &&
      theta < thetaMax &&
      dist >= distMin &&
      dist >= distMax
  }


  def thetaMin: Double

  def thetaMax: Double

  def distMin: Double

  def distMax: Double

}

sealed trait BelowCornerShotZone extends ShotZone {
  override def isInZone(shot: RawShotData): Boolean =
    ShotZone.shotValue(shot) == value &&
      shot.xCoordinate.intValue() < xMax &&
      shot.xCoordinate.intValue() >= xMin

  def xMax: Int

  def xMin: Int
}

object ShotZone {

  def findShotZone(shot: RawShotData): ShotZone =
    if (RestrictedArea.isInZone(shot)) {
      RestrictedArea
    } else if(shot.yCoordinate <= 92.5) {
      BelowCornerShotZones.find(_.isInZone(shot)).head
    } else {
      AboveCornerShotZones.find(_.isInZone(shot)).head
    }

  private val BelowCornerShotZones: Seq[BelowCornerShotZone] =
    Seq(LeftCorner, RightCorner)

  private val AboveCornerShotZones: Seq[AboveCornerShotZone] =
    Seq(Long3Left, Long3Right, Mid3CenterLeft, Mid3CenterRight, Mid3Left, Mid3Right)

  def theta(shot: RawShotData): Double =
    math.atan2(shot.yCoordinate.intValue(), shot.xCoordinate.intValue())

  def distance(shot: RawShotData): Double =
    math.hypot(shot.xCoordinate.intValue(), shot.yCoordinate.intValue())

  def shotValue(shot: RawShotData): Int =
    shot.shotZoneBasic.substring(0, 1).toInt


  case object RestrictedArea extends ShotZone {
    override val value: Int = 2
    override def isInZone(shot: RawShotData): Boolean = distance(shot) <= 4
  }

  case object LeftCorner extends BelowCornerShotZone {
    override val value: Int = 3
    override val xMax: Int = -215
    override val xMin: Int = -400
  }
  case object RightCorner extends BelowCornerShotZone {
    override val value: Int = 3
    override val xMax: Int = 400
    override val xMin: Int = 215
  }

  case object Mid3Left extends AboveCornerShotZone {
    override def thetaMin: Double = 0

    override def thetaMax: Double = 60

    override def distMin: Double = 22

    override def distMax: Double = 27

    override def value: Int = 3
  }

  case object Mid3CenterLeft extends AboveCornerShotZone {
    override def thetaMin: Double = 60

    override def thetaMax: Double = 90

    override def distMin: Double = 22

    override def distMax: Double = 27

    override def value: Int = 3
  }

  case object Mid3CenterRight extends AboveCornerShotZone {
    override def thetaMin: Double = 90

    override def thetaMax: Double = 120

    override def distMin: Double = 22

    override def distMax: Double = 27

    override def value: Int = 3
  }

  case object Mid3Right extends AboveCornerShotZone {
    override def thetaMin: Double = 120

    override def thetaMax: Double = 180

    override def distMin: Double = 22

    override def distMax: Double = 27

    override def value: Int = 3
  }

  case object Long3Left extends AboveCornerShotZone {
    override def thetaMin: Double = 0

    override def thetaMax: Double = 90

    override def distMin: Double = 27

    override def distMax: Double = 100

    override def value: Int = 3
  }

  case object Long3Right extends AboveCornerShotZone {
    override def thetaMin: Double = 90

    override def thetaMax: Double = 180

    override def distMin: Double = 27

    override def distMax: Double = 100

    override def value: Int = 3
  }

}