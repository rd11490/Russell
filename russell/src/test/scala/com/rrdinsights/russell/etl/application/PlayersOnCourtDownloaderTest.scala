package com.rrdinsights.russell.etl.application

import com.rrdinsights.russell.TestSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class PlayersOnCourtDownloaderTest extends TestSpec {
  test("filter Players") {

    val expected: Seq[Integer] = Seq(
      1627767, 1628960, 203497, 201937, 1628378, 203109
    )

    val subsIn: Seq[(Integer, ((Integer, Integer), String))] = Seq(
      ((0, 0), 1627767),
      ((1,1), 1628960),
      ((2, 2), 203957),
      ((3, 3), 203497),
      ((4,4), 201937),
      ((5, 5), 1628378),
      ((6, 6), 203109),
      ((7, 8), 1627779)
    ).map(v => (Integer.valueOf(v._2), ((Integer.valueOf(v._1._1), Integer.valueOf(v._1._2)), "IN")))

    val subsOut: Seq[(Integer, ((Integer, Integer), String))] = Seq(
      ((0, 0), 1626143),
      ((1, 1), 203957),
      ((2, 2), 202692),
      ((3, 3), 202327),
      ((4, 4), 203957),
      ((5, 5), 204060),
      ((6, 6), 1627777),
      ((7, 7), 1627779)

    ).map(v => (Integer.valueOf(v._2), ((Integer.valueOf(v._1._1), Integer.valueOf(v._1._2)), "OUT")))

    val playersToFilter = PlayersOnCourtDownloader.playersToIgnoreCalc(subsIn, subsOut)

    assert(playersToFilter.sorted === expected.sorted)

  }
}
