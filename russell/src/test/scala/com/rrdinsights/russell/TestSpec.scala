package com.rrdinsights.russell

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FunSuite, Matchers}

abstract class TestSpec extends FunSuite with TypeCheckedTripleEquals with Matchers with MockitoSugar {

}
