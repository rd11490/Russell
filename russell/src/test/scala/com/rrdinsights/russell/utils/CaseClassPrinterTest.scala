package com.rrdinsights.russell.utils

import com.rrdinsights.russell.TestSpec
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class CaseClassPrinterTest extends TestSpec {

  import com.rrdinsights.russell.utils.CaseClassPrinter._
  test("print") {
    val test1 = Test()
    val out = test1.toPrettyString[Test]
    assert(
      out === "Test(boolean = true, int = 5, string = \"This is a string\")")
  }
}

private final case class Test(boolean: Boolean = true,
                              int: Int = 5,
                              string: String = "This is a string")
