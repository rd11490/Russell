package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.TestSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class PlayerOnCourtDownloaderTest extends TestSpec {

  test("minutesInPeriod") {
    assert(PlayersOnCourtDownloader.minutesInPeriod(1) === 12)
    assert(PlayersOnCourtDownloader.minutesInPeriod(2) === 12)
    assert(PlayersOnCourtDownloader.minutesInPeriod(3) === 12)
    assert(PlayersOnCourtDownloader.minutesInPeriod(4) === 12)
    assert(PlayersOnCourtDownloader.minutesInPeriod(5) === 5)
    assert(PlayersOnCourtDownloader.minutesInPeriod(6) === 5)
    assert(PlayersOnCourtDownloader.minutesInPeriod(7) === 5)
  }

  test("periodToMinutesPlayed") {
    assert(PlayersOnCourtDownloader.periodToMinutesPlayed(1) === 0)
    assert(PlayersOnCourtDownloader.periodToMinutesPlayed(2) === 12)
    assert(PlayersOnCourtDownloader.periodToMinutesPlayed(3) === 24)
    assert(PlayersOnCourtDownloader.periodToMinutesPlayed(4) === 36)
    assert(PlayersOnCourtDownloader.periodToMinutesPlayed(5) === 48)
    assert(PlayersOnCourtDownloader.periodToMinutesPlayed(6) === 53)
    assert(PlayersOnCourtDownloader.periodToMinutesPlayed(7) === 58)
  }

  test("timeFromStartOfGame") {
    assert(PlayersOnCourtDownloader.timeFromStartOfGame(1, 12, 0) === 0)
    assert(PlayersOnCourtDownloader.timeFromStartOfGame(1, 0, 0) === 720)
    assert(PlayersOnCourtDownloader.timeFromStartOfGame(1, 11, 59) === 1)
    assert(PlayersOnCourtDownloader.timeFromStartOfGame(1, 11, 50) === 10)
    assert(PlayersOnCourtDownloader.timeFromStartOfGame(1, 5, 30) === 390)
    assert(PlayersOnCourtDownloader.timeFromStartOfGame(5, 4, 30) === 2910)
  }

  test("timeFromStartOfGameAtPeriod") {
    val period = 1
    assert(PlayersOnCourtDownloader.timeFromStartOfGameAtPeriod(period) === 25)

  }
}
