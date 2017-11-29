package com.rrdinsights.russell.utils

import com.rrdinsights.russell.TestSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class MapJoinTest extends TestSpec {
  test("join") {
    val left = Map(("A", "AV1"),("B", "BV1"), ("C", "CV1"))
    val right = Map(("A", "AV2"),("B", "BV2"))

    val joined = MapJoin.join(left, right)

    assert(joined.size === 2)
    assert(joined.head === ("AV1", "AV2"))
    assert(joined(1) === ("BV1", "BV2"))
  }

  test("leftOuterJoin") {
    val left = Map(("A", "AV1"),("B", "BV1"), ("C", "CV1"))
    val right = Map(("A", "AV2"),("B", "BV2"))

    val joined = MapJoin.leftOuterJoin(left, right)

    assert(joined.size === 3)
    assert(joined.head === ("AV1", Some("AV2")))
    assert(joined(1) === ("BV1", Some("BV2")))
    assert(joined(2) === ("CV1", None))
  }
}
