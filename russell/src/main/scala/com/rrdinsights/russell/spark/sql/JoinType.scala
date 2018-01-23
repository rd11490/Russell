package com.rrdinsights.russell.spark.sql

sealed trait JoinType {

  def name: String

  override final def toString: String = name
}

object JoinType {

  /** Default join type */
  case object Inner extends JoinType {
    override val name: String = "inner"
  }

  case object LeftOuter extends JoinType {
    override val name: String = "left_outer"
  }

  case object RightOuter extends JoinType {
    override val name: String = "right_outer"
  }

  case object FullOuter extends JoinType {
    override val name: String = "full_outer"
  }

  case object Cross extends JoinType {
    override val name: String = "cross"
  }
}