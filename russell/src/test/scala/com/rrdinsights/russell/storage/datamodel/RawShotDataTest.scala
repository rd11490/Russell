package com.rrdinsights.russell.storage.datamodel

import com.rrdinsights.russell.TestSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class RawShotDataTest extends TestSpec {
  test("gameId To Season") {
    assert(RawShotData.gameIdToSeason("0021400574") === "2014-15")
    assert(RawShotData.gameIdToSeason("0021500997") === "2015-16")
    assert(RawShotData.gameIdToSeason("0021500106") === "2015-16")
    assert(RawShotData.gameIdToSeason("0011600052") === "2016-17")
    assert(RawShotData.gameIdToSeason("0021600782") === "2016-17")
  }
}
