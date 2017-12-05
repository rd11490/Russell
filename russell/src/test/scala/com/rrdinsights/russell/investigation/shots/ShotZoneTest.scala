package com.rrdinsights.russell.investigation.shots

import com.rrdinsights.russell.TestSpec
import com.rrdinsights.russell.storage.datamodel.RawShotData
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class ShotZoneTest extends TestSpec {

  test("findShotZone") {
    val leftCorner1 = buildRawShot(-245, 62, 3)
    val leftCorner2 = buildRawShot(-220, -35, 3)

    assert(ShotZone.findShotZone(leftCorner1) === ShotZone.LeftCorner)
    assert(ShotZone.findShotZone(leftCorner2) === ShotZone.LeftCorner)

    val rightCorner1 = buildRawShot(245, 62, 3)
    val rightCorner2 = buildRawShot(220, -35, 3)

    assert(ShotZone.findShotZone(rightCorner1) === ShotZone.RightCorner)
    assert(ShotZone.findShotZone(rightCorner2) === ShotZone.RightCorner)

    val left2ShortBaseline = buildRawShot(-37, -19, 2)
    val left2Baseline = buildRawShot(-161, -13, 2)
    val left3 = buildRawShot(-189, 175, 3)
    val leftCenter3 = buildRawShot(-68, 248, 3)

    val center3 = buildRawShot(0, 350, 3)

    val rightCenter3 = buildRawShot(75, 235, 3)
    val right3 = buildRawShot(207, 145, 3)
    val right2Baseline = buildRawShot(161, -13, 2)

    printShotInfo(left2ShortBaseline)
    printShotInfo(left2Baseline)
    printShotInfo(left3)
    printShotInfo(leftCenter3)

    printShotInfo(center3)

    printShotInfo(rightCenter3)
    printShotInfo(right3)
    printShotInfo(right2Baseline)

    assert(ShotZone.findShotZone(left2Baseline) == ShotZone.LeftBaseline18FT)
    assert(ShotZone.findShotZone(left3) === ShotZone.Left27FT)
    assert(ShotZone.findShotZone(leftCenter3) === ShotZone.Left27FT)
    assert(ShotZone.findShotZone(rightCenter3) === ShotZone.Right27FT)
    assert(ShotZone.findShotZone(right3) === ShotZone.Right27FT)
    assert(ShotZone.findShotZone(right2Baseline) == ShotZone.RightBaseline18FT)

  }

  private def printShotInfo(shot: RawShotData): Unit =
    println(shot.xCoordinate, shot.yCoordinate,
      ShotZone.distance(shot.xCoordinate, shot.yCoordinate),
      ShotZone.theta(shot.xCoordinate, shot.yCoordinate))

  private def buildRawShot(x: Int, y: Int, value: Int): RawShotData = RawShotData(
    null, null, null, null, null, null, null, null, null, null, null, null,
    null, s"${value}PT Shot", null, null, null, null, x, y, null, null, null, null, null, null, null)

}
