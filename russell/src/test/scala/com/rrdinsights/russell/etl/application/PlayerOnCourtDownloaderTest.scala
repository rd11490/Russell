package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.TestSpec
import com.rrdinsights.russell.storage.datamodel.RawShotData
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
    assert(PlayersOnCourtDownloader.timeFromStartOfGame(buildRawShotData(1, 12, 0)) === 0)
    assert(PlayersOnCourtDownloader.timeFromStartOfGame(buildRawShotData(1, 11, 59)) === 1)
    assert(PlayersOnCourtDownloader.timeFromStartOfGame(buildRawShotData(1, 11, 50)) === 10)
    assert(PlayersOnCourtDownloader.timeFromStartOfGame(buildRawShotData(1, 5, 30)) === 390)
    assert(PlayersOnCourtDownloader.timeFromStartOfGame(buildRawShotData(5, 4, 30)) === 2910)
  }

  private def buildRawShotData(period: Int, minutesRemaining: Int, secondsRemaining: Int): RawShotData =
    RawShotData(null, null, null, null, null, null, null, null, period, minutesRemaining = minutesRemaining, secondsRemaining = secondsRemaining, null,
      null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)
}
