package com.rrdinsights.russell.utils

import com.rrdinsights.russell.TestSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CredsTest extends TestSpec {

  test("Read Creds") {
    val creds = Creds.getCreds
    assert(creds != null)
    assert(creds.MySQL != null)
  }
}