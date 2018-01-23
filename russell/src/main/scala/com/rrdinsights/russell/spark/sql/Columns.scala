package com.rrdinsights.russell.spark.sql

import org.apache.spark.sql.{Column, functions}

object Columns {
  val eventId: Column = column("eventId")
  val xLoc: Column = column("xLoc")
  val yLoc: Column = column("yLoc")
  val gameClock: Column = column("gameClock")
  val game_clock: Column = column("game_clock")
  val event_id: Column = column("event_id")
  val quarter: Column = column("quarter")


  private def column(name: String): Column = functions.col(name)
}
