package com.rrdinsights.russell.utils

import com.rrdinsights.russell.TestSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class TimeUtilsTest extends TestSpec {

  test("minutesInPeriod") {
    assert(TimeUtils.minutesInPeriod(1) === 12)
    assert(TimeUtils.minutesInPeriod(2) === 12)
    assert(TimeUtils.minutesInPeriod(3) === 12)
    assert(TimeUtils.minutesInPeriod(4) === 12)
    assert(TimeUtils.minutesInPeriod(5) === 5)
    assert(TimeUtils.minutesInPeriod(6) === 5)
    assert(TimeUtils.minutesInPeriod(7) === 5)
  }

  test("periodToMinutesPlayed") {
    assert(TimeUtils.periodToMinutesPlayed(1) === 0)
    assert(TimeUtils.periodToMinutesPlayed(2) === 12)
    assert(TimeUtils.periodToMinutesPlayed(3) === 24)
    assert(TimeUtils.periodToMinutesPlayed(4) === 36)
    assert(TimeUtils.periodToMinutesPlayed(5) === 48)
    assert(TimeUtils.periodToMinutesPlayed(6) === 53)
    assert(TimeUtils.periodToMinutesPlayed(7) === 58)
  }

  test("timeFromStartOfGame") {
    assert(TimeUtils.timeFromStartOfGame(1, 12, 0) === 0)
    assert(TimeUtils.timeFromStartOfGame(1, 0, 0) === 720)
    assert(TimeUtils.timeFromStartOfGame(1, 11, 59) === 1)
    assert(TimeUtils.timeFromStartOfGame(1, 11, 50) === 10)
    assert(TimeUtils.timeFromStartOfGame(1, 5, 30) === 390)
    assert(TimeUtils.timeFromStartOfGame(5, 4, 30) === 2910)
  }

  test("timeFromStartOfGameAtPeriod") {
    assert(TimeUtils.timeFromStartOfGameAtPeriod(1) === 0)
    assert(TimeUtils.timeFromStartOfGameAtPeriod(2) === 720)
    assert(TimeUtils.timeFromStartOfGameAtPeriod(3) === 1440)
    assert(TimeUtils.timeFromStartOfGameAtPeriod(4) === 2160)
    assert(TimeUtils.timeFromStartOfGameAtPeriod(5) === 2880)
    assert(TimeUtils.timeFromStartOfGameAtPeriod(6) === 3180)

  }

  test("sort dates") {
    val d1 = (1, "Nov 18, 2017")
    val d2 = (2, "OCT 20, 2017")
    val d3 = (3, "Dec 21, 2017")

    val dates = Seq(d3, d1, d2)

    val out = dates
      .map(v => (v._1, TimeUtils.parseGameLogDate(v._2)))
      .sortBy(_._2)

    assert(out.head._1 == 2)
    assert(out.last._1 == 3)
  }

//  test("parse game date") {
//    val testDate = "THURSDAY, DECEMBER 9, 2004"
//    assert(TimeUtils.parseGameDate(testDate) === 1102550400000L)
//  }
}
