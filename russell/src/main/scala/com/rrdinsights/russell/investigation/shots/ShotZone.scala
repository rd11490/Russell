package com.rrdinsights.russell.investigation.shots

import com.rrdinsights.russell.storage.datamodel.RawShotData

sealed trait ShotZone {
  def isInZone(x: Integer, y: Integer, shotValue: Integer): Boolean

  def value: Int
}

sealed trait NonCornerShotZone extends ShotZone {
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

sealed trait CornerShotZone extends ShotZone {
  override def isInZone(x: Integer, y: Integer, shotValue: Integer): Boolean =
    shotValue == value &&
      x.intValue() < xMax &&
      x.intValue() >= xMin

  def xMax: Int

  def xMin: Int
}

object ShotZone {

  private val CornerShotZones: Seq[CornerShotZone] =
    Seq(
      LeftCorner,
      RightCorner)

  private val NonCornerShotZones: Seq[NonCornerShotZone] =
    Seq(
      LeftBaseline11FT, Left11FT, Right11FT, RightBaseline11FT,
      LeftBaseline18FT, Left18FT, Right18FT, RightBaseline18FT,
      LeftBaseline23FT, Left23FT, Right23FT, RightBaseline23FT,
      Left27FT, Right27FT,
      LeftLong3, RightLong3)

  def find(str: String): ShotZone =
    (CornerShotZones ++ NonCornerShotZones :+ RestrictedArea)
      .find(_.toString == str)
      .getOrElse(throw new IllegalArgumentException(s"$str is not a valid Shot Zone"))

  def findShotZone(shot: RawShotData): ShotZone = {
    val shotValue = ShotZone.shotValue(shot)
    try {
      findShotZone(shot.xCoordinate, shot.yCoordinate, shotValue)
    } catch {
      case e: Throwable =>
        println()
        println(shot)
        println(shot.xCoordinate, shot.yCoordinate, shotValue)
        println(distance(shot.xCoordinate, shot.yCoordinate))
        println(theta(shot.xCoordinate, shot.yCoordinate))
        throw e
    }
  }


  def findShotZone(x: Integer, y: Integer, value: Integer): ShotZone = {
    try {
      if (RestrictedArea.isInZone(x, y, value)) {
        RestrictedArea
      } else if (y <= 92.5 && value == 3) {
        CornerShotZones.find(_.isInZone(x, y, value)).head
      } else {
        NonCornerShotZones.find(_.isInZone(x, y, value)).head
      }
    } catch {
      case t: Throwable =>
        println(x)
        println(y)
        print(value)
        throw t
    }
  }



  def theta(x: Integer, y: Integer): Double = {
    val angle = math.toDegrees(math.atan2(y.intValue(), x.intValue()))
    if (x < 0 && angle < 0) {
      360 + angle
    } else {
      angle
    }
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

  case object LeftCorner extends CornerShotZone {
    override val value: Int = 3
    override val xMax: Int = -190
    override val xMin: Int = -500
  }

  case object RightCorner extends CornerShotZone {
    override val value: Int = 3
    override val xMax: Int = 500
    override val xMin: Int = 190
  }

  case object RightBaseline11FT extends NonCornerShotZone {
    override val thetaMin: Double = -90
    override val thetaMax: Double = 23

    override val distMin: Double = 4
    override val distMax: Double = 11

    override val value: Int = 2
  }

  case object Right11FT extends NonCornerShotZone {
    override val thetaMin: Double = 23
    override val thetaMax: Double = 90

    override val distMin: Double = 4
    override val distMax: Double = 11

    override val value: Int = 2
  }

  case object Left11FT extends NonCornerShotZone {
    override val thetaMin: Double = 90
    override val thetaMax: Double = 157

    override val distMin: Double = 4
    override val distMax: Double = 11

    override val value: Int = 2
  }

  case object LeftBaseline11FT extends NonCornerShotZone {
    override val thetaMin: Double = 157
    override val thetaMax: Double = 270

    override val distMin: Double = 4
    override val distMax: Double = 11

    override val value: Int = 2
  }

  case object RightBaseline18FT extends NonCornerShotZone {
    override val thetaMin: Double = -90
    override val thetaMax: Double = 23

    override val distMin: Double = 10
    override val distMax: Double = 18

    override val value: Int = 2
  }

  case object Right18FT extends NonCornerShotZone {
    override val thetaMin: Double = 23
    override val thetaMax: Double = 90

    override val distMin: Double = 10
    override val distMax: Double = 18

    override val value: Int = 2
  }

  case object Left18FT extends NonCornerShotZone {
    override val thetaMin: Double = 90
    override val thetaMax: Double = 157

    override val distMin: Double = 10
    override val distMax: Double = 18

    override val value: Int = 2
  }

  case object LeftBaseline18FT extends NonCornerShotZone {
    override val thetaMin: Double = 157
    override val thetaMax: Double = 270

    override val distMin: Double = 10
    override val distMax: Double = 18

    override val value: Int = 2
  }

  case object RightBaseline23FT extends NonCornerShotZone {
    override val thetaMin: Double = -90
    override val thetaMax: Double = 23

    override val distMin: Double = 18
    override val distMax: Double = 27

    override val value: Int = 2
  }

  case object Right23FT extends NonCornerShotZone {
    override val thetaMin: Double = 23
    override val thetaMax: Double = 90

    override val distMin: Double = 18
    override val distMax: Double = 27

    override val value: Int = 2
  }

  case object Left23FT extends NonCornerShotZone {
    override val thetaMin: Double = 90
    override val thetaMax: Double = 157

    override val distMin: Double = 18
    override val distMax: Double = 27

    override val value: Int = 2
  }

  case object LeftBaseline23FT extends NonCornerShotZone {
    override val thetaMin: Double = 157
    override val thetaMax: Double = 270

    override val distMin: Double = 18
    override val distMax: Double = 27

    override val value: Int = 2
  }

  case object Right27FT extends NonCornerShotZone {
    override val thetaMin: Double = -90
    override val thetaMax: Double = 90

    override val distMin: Double = 20
    override val distMax: Double = 27

    override val value: Int = 3
  }

  case object Left27FT extends NonCornerShotZone {
    override val thetaMin: Double = 90
    override val thetaMax: Double = 270

    override val distMin: Double = 20
    override val distMax: Double = 27

    override val value: Int = 3
  }

  case object RightLong3 extends NonCornerShotZone {
    override val thetaMin: Double = -90
    override val thetaMax: Double = 90

    override val distMin: Double = 27
    override val distMax: Double = 100

    override val value: Int = 3
  }

  case object LeftLong3 extends NonCornerShotZone {
    override val thetaMin: Double = 90
    override val thetaMax: Double = 270

    override val distMin: Double = 27
    override val distMax: Double = 100

    override val value: Int = 3
  }


}