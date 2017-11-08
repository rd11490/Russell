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


    val left3 = buildRawShot(-189, 175, 3)
    val leftCenter3 = buildRawShot(-68, 248, 3)
    val rightCenter3 = buildRawShot(129, 213, 3)
    val right3 = buildRawShot(207, 145, 3)

    assert(ShotZone.findShotZone(left3) === ShotZone.Mid3Left)
    assert(ShotZone.findShotZone(leftCenter3) === ShotZone.Mid3CenterLeft)
    assert(ShotZone.findShotZone(rightCenter3) === ShotZone.Mid3CenterRight)
    assert(ShotZone.findShotZone(right3) === ShotZone.Mid3Right)

  }

  private def buildRawShot(x: Int, y: Int, value: Int): RawShotData = RawShotData(
    null, null, null, null, null, null, null, null, null, null, null, null,
    null, s"${value}PT Shot", null, null, null, null, x, y, null, null, null, null, null, null, null)

}
