package com.rrdinsights.russell.spark.sql

sealed trait JoinType {

  def name: String

  override final def toString: String = name
}

object JoinType {

  /** Default join type */
  case object Inner extends JoinType {
    override val name: String = "Inner"
  }

  case object LeftOuter extends JoinType {
    override val name: String = "LeftOuter"
  }

  case object RightOuter extends JoinType {
    override val name: String = "RightOuter"
  }

  case object FullOuter extends JoinType {
    override val name: String = "FullOuter"
  }
}