package com.rrdinsights.russell.investigation.shots

import com.rrdinsights.russell.TestSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExpectedShotsCalculatorPredictiveTest extends TestSpec {

  test("parse Date") {
    val s = "OCT 22, 2016"
    val out = ExpectedShotsCalculatorPredictive.parseDate(s)

    assert(out.toString === "2016-10-22T04:00:00Z")
  }
}
