package com.rrdinsights.russell.utils

import com.rrdinsights.russell.TestSpec

class CredsTest extends TestSpec {

  test("Read Creds") {
    val creds = Creds.getCreds
    assert(creds != null)
    assert(creds.MySQL != null)
  }
}
