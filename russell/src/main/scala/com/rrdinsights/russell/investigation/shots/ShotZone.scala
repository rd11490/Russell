package com.rrdinsights.russell.investigation.shots

import com.rrdinsights.russell.storage.datamodel.RawShotData

sealed trait ShotZone {
  def isInZone(x: Integer, y: Integer, shotValue: Integer): Boolean

  def value: Int
}

sealed trait AboveCornerShotZone extends ShotZone {
  override def isInZone(x: Integer, y: Integer, shotValue: Integer): Boolean = {
    val theta = ShotZone.theta(x, y)
    val dist = ShotZone.distance(x, y)

    shotValue == value &&
      theta >= thetaMin &&
      theta < thetaMax &&
      dist >= distMin &&
      dist < distMax
  }


  def thetaMin: Double

  def thetaMax: Double

  def distMin: Double

  def distMax: Double

}

sealed trait BelowCornerShotZone extends ShotZone {
  override def isInZone(x: Integer, y: Integer, shotValue: Integer): Boolean =
    shotValue == value &&
      x.intValue() < xMax &&
      x.intValue() >= xMin

  def xMax: Int

  def xMin: Int
}

object ShotZone {

  private val BelowCornerShotZones: Seq[BelowCornerShotZone] =
    Seq(
      LeftCorner, LeftLongMidBaseLine, LeftMidBaseLine, LeftShortBaseLine, LeftPaint,
      RightCorner, RightLongBaseLine, RightMidBaseLine, RightShortBaseLine, RightPaint)

  private val AboveCornerShotZones: Seq[AboveCornerShotZone] =
    Seq(Long3Left, Long3Right, Mid3Left, Mid3CenterLeft, Mid3CenterRight, Mid3Right,
      Long2Left, Long2CenterLeft, Long2CenterRight, Long2Right,
      Mid2Left, Mid2CenterLeft, Mid2CenterRight, Mid2Right,
      Short2Left, Short2CenterLeft, Short2CenterRight, Short2Right)

  def find(str: String): ShotZone =
    (BelowCornerShotZones ++ AboveCornerShotZones :+ RestrictedArea)
      .find(_.toString == str)
      .getOrElse(throw new IllegalArgumentException(s"$str is not a valid Shot Zone"))

  def findShotZone(shot: RawShotData): ShotZone = {
    val shotValue = ShotZone.shotValue(shot)
    findShotZone(shot.xCoordinate, shot.yCoordinate, shotValue)
  }


  def findShotZone(x: Integer, y: Integer, value: Integer): ShotZone = {
    try {
      if (RestrictedArea.isInZone(x, y, value)) {
        RestrictedArea
      } else if (y <= 92.5) {
        BelowCornerShotZones.find(_.isInZone(x, y, value)).head
      } else {
        AboveCornerShotZones.find(_.isInZone(x, y, value)).head
      }
    } catch {
      case t: Throwable =>
        println(x)
        println(y)
        throw t
    }
  }



  def theta(x: Integer, y: Integer): Double = {
    180 - math.toDegrees(math.atan2(y.intValue(), x.intValue()))
  }

  def distance(x: Integer, y: Integer): Double =
    shotLocToReal(math.hypot(x.intValue(), y.intValue())) / 12.0

  private def shotLocToReal(inches: Double): Double =
    (inches / 7.5) * 9.0

  def shotValue(shot: RawShotData): Int =
    shot.shotZoneBasic.substring(0, 1).toInt

  private def realToShotLoc(inches: Double): Double =
    (inches / 9.0) * 7.5

  case object RestrictedArea extends ShotZone {
    override val value: Int = 2

    override def isInZone(x: Integer, y: Integer, value: Integer): Boolean = distance(x, y) <= 4
  }

  case object LeftCorner extends BelowCornerShotZone {
    override val value: Int = 3
    override val xMax: Int = -200
    override val xMin: Int = -400
  }

  case object LeftLongMidBaseLine extends BelowCornerShotZone {
    override val value: Int = 2
    override val xMax: Int = -178
    override val xMin: Int = -250
  }

  case object LeftMidBaseLine extends BelowCornerShotZone {
    override val value: Int = 2
    override val xMax: Int = -137
    override val xMin: Int = -178
  }

  case object LeftShortBaseLine extends BelowCornerShotZone {
    override val value: Int = 2
    override val xMax: Int = -80
    override val xMin: Int = -137
  }

  case object LeftPaint extends BelowCornerShotZone {
    override val value: Int = 2
    override val xMax: Int = 0
    override val xMin: Int = -80
  }

  case object RightCorner extends BelowCornerShotZone {
    override val value: Int = 3
    override val xMax: Int = 400
    override val xMin: Int = 200
  }

  case object RightLongBaseLine extends BelowCornerShotZone {
    override val value: Int = 2
    override val xMax: Int = 250
    override val xMin: Int = 178
  }

  case object RightMidBaseLine extends BelowCornerShotZone {
    override val value: Int = 2
    override val xMax: Int = 178
    override val xMin: Int = 137
  }

  case object RightShortBaseLine extends BelowCornerShotZone {
    override val value: Int = 2
    override val xMax: Int = 137
    override val xMin: Int = 80
  }

  case object RightPaint extends BelowCornerShotZone {
    override val value: Int = 2
    override val xMax: Int = 80
    override val xMin: Int = 0
  }

  case object Mid3Left extends AboveCornerShotZone {
    override val thetaMin: Double = 0
    override val thetaMax: Double = 60

    override val distMin: Double = 20
    override val distMax: Double = 27

    override val value: Int = 3
  }

  case object Mid3CenterLeft extends AboveCornerShotZone {
    override val thetaMin: Double = 60

    override val thetaMax: Double = 90

    override val distMin: Double = 20

    override val distMax: Double = 27

    override val value: Int = 3
  }

  case object Mid3CenterRight extends AboveCornerShotZone {
    override val thetaMin: Double = 90

    override val thetaMax: Double = 120

    override val distMin: Double = 20

    override val distMax: Double = 27

    override val value: Int = 3
  }

  case object Mid3Right extends AboveCornerShotZone {
    override val thetaMin: Double = 120

    override val thetaMax: Double = 180

    override val distMin: Double = 20

    override val distMax: Double = 27

    override val value: Int = 3
  }

  case object Long3Left extends AboveCornerShotZone {
    override val thetaMin: Double = 0

    override val thetaMax: Double = 90

    override val distMin: Double = 27

    override val distMax: Double = 100

    override val value: Int = 3
  }

  case object Long3Right extends AboveCornerShotZone {
    override val thetaMin: Double = 90

    override val thetaMax: Double = 180

    override val distMin: Double = 27

    override val distMax: Double = 100

    override val value: Int = 3
  }


  case object Long2Left extends AboveCornerShotZone {
    override val thetaMin: Double = 0

    override val thetaMax: Double = 60

    override val distMin: Double = 20

    override val distMax: Double = 27

    override val value: Int = 2
  }

  case object Long2CenterLeft extends AboveCornerShotZone {
    override val thetaMin: Double = 60

    override val thetaMax: Double = 90

    override val distMin: Double = 20

    override val distMax: Double = 27

    override val value: Int = 2
  }

  case object Long2CenterRight extends AboveCornerShotZone {
    override val thetaMin: Double = 90

    override val thetaMax: Double = 120

    override val distMin: Double = 20

    override val distMax: Double = 27

    override val value: Int = 2
  }

  case object Long2Right extends AboveCornerShotZone {
    override val thetaMin: Double = 120

    override val thetaMax: Double = 180

    override val distMin: Double = 20

    override val distMax: Double = 27

    override val value: Int = 2
  }

  case object Mid2Left extends AboveCornerShotZone {
    override val thetaMin: Double = 0

    override val thetaMax: Double = 60

    override val distMin: Double = 16

    override val distMax: Double = 20

    override val value: Int = 2
  }

  case object Mid2CenterLeft extends AboveCornerShotZone {
    override val thetaMin: Double = 60

    override val thetaMax: Double = 90

    override val distMin: Double = 16

    override val distMax: Double = 20

    override val value: Int = 2
  }

  case object Mid2CenterRight extends AboveCornerShotZone {
    override val thetaMin: Double = 90

    override val thetaMax: Double = 120

    override val distMin: Double = 16

    override val distMax: Double = 20

    override val value: Int = 2
  }

  case object Mid2Right extends AboveCornerShotZone {
    override val thetaMin: Double = 120
    override val thetaMax: Double = 180

    override val distMin: Double = 16
    override val distMax: Double = 20

    override val value: Int = 2
  }

  case object Short2Left extends AboveCornerShotZone {
    override val thetaMin: Double = 0
    override val thetaMax: Double = 60

    override val distMin: Double = 0
    override val distMax: Double = 16

    override val value: Int = 2
  }

  case object Short2CenterLeft extends AboveCornerShotZone {
    override val thetaMin: Double = 60
    override val thetaMax: Double = 90

    override val distMin: Double = 0
    override val distMax: Double = 16

    override val value: Int = 2
  }

  case object Short2CenterRight extends AboveCornerShotZone {
    override val thetaMin: Double = 90
    override val thetaMax: Double = 120

    override val distMin: Double = 0
    override val distMax: Double = 16

    override val value: Int = 2
  }

  case object Short2Right extends AboveCornerShotZone {
    override val thetaMin: Double = 120
    override val thetaMax: Double = 180

    override val distMin: Double = 0
    override val distMax: Double = 16

    override val value: Int = 2
  }

}