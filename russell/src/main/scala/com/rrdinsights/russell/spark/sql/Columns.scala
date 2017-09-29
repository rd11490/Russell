package com.rrdinsights.russell.spark.sql

import org.apache.spark.sql.{Column, functions}

object Columns {
  val eventId: Column = column("eventId")
  val xLoc: Column = column("xLoc")
  val yLoc: Column = column("yLoc")
  val gameClock: Column = column("gameClock")

  private def column(name: String): Column = functions.col(name)
}
