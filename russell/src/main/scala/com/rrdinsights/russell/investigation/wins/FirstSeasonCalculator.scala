package com.rrdinsights.russell.investigation.wins

import com.rrdinsights.russell.investigation.PlayerMapper
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.RealAdjustedFourFactors
import com.rrdinsights.russell.storage.tables.NBATables

object FirstSeasonCalculator {

  def main(strings: Array[String]): Unit = {
    val fourFactors = MySqlClient.selectFrom(
      NBATables.real_adjusted_four_factors,
      RealAdjustedFourFactors.apply)
    val firstSeason = fourFactors.map(_.season).distinct.min

    val filtered = fourFactors.groupBy(v => (v.playerId, v.playerName))
      .filterNot(_._2.exists(_.season == firstSeason))

    println(fourFactors.size)
    println(filtered.size)

    val out = filtered
      .flatMap(v => {
        val seasons = v._2.filter(_.season != firstSeason)
        if (seasons.isEmpty) {
          None
        } else {
          Some(seasons.minBy(_.season))
        }
      })
      .map(v => (v.playerName, v.season, v.LA_RAPM.doubleValue(), v.LA_RAPM__Off.doubleValue(), v.LA_RAPM__Def.doubleValue()))

    println("NET")
    println(out.map(_._3).sum/out.size)

    println("Off")
    println(out.map(_._4).sum/out.size)

    println("Def")
    println(out.map(_._5).sum/out.size)

  }

}

