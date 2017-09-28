package com.rrdinsights.russell.storage.datamodel

object DataModelUtils {
  private[datamodel] def gameIdToSeason(gameId: String): String = {
    val year = gameId.substring(3,5)
    val yearInt = Integer.valueOf(year)
    if (yearInt > 50) { // 1900s games
      if (yearInt == 99) {
        s"19$year-00"
      } else {
        s"19$year-${yearInt+1}"
      }
    } else { //2000 games
      if (yearInt < 9) {
        s"20$year-0${yearInt+1}"
      } else {
        s"20$year-${yearInt+1}"
      }
    }
  }
}
