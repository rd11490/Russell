package com.rrdinsights.russell.utils

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object TimeUtils {

  private val Formatter: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

  def dtNow: String = LocalDateTime.now().format(Formatter)


  def timeFromStartOfGame(period: Int, minutesRemaining: Int, secondsRemaining: Int): Int = {
    val previousPeriods = periodToMinutesPlayed(period) * 60
    val minutesElapsedThisPeriod = (minutesInPeriod(period) - minutesRemaining - 1) * 60
    val secondsElapsed = 60 - secondsRemaining

    previousPeriods + minutesElapsedThisPeriod + secondsElapsed
  }

  def convertTimeStringToTime(period: Int, time: String): Int = {
    val splitTime = time.split(":", 2)
    val minutesLeft = splitTime(0).toInt
    val secodsLeft = splitTime(1).toInt

    timeFromStartOfGame(period, minutesLeft, secodsLeft)
  }

  def timeFromStartOfGameAtPeriod(period: Int): Int =
    timeFromStartOfGame(period, minutesInPeriod(period)-1, 35)

  private[utils] def periodToMinutesPlayed(period: Int): Int =
    if (period > 4) {
      (4 * 12) + ((period - 5) * 5)
    } else {
      (period - 1) * 12
    }

  private[utils] def minutesInPeriod(period: Int): Int =
    if (period > 4) {
      5
    } else {
      12
    }

}
