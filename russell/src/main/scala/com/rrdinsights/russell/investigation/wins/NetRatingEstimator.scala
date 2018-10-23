package com.rrdinsights.russell.investigation.wins

import com.rrdinsights.russell.investigation.TeamMapper
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{
  LeagueSchedule,
  MinuteProjection,
  RealAdjustedFourFactors
}
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.MapJoin

object NetRatingEstimator {

  def main(strings: Array[String]): Unit = {
    val fourFactors = MySqlClient
      .selectFrom(NBATables.real_adjusted_four_factors_multi,
                  RealAdjustedFourFactors.apply,
                  "season = '2016-17,2017-18'")
      .map(v => (v.playerName, v))
      .toMap

    val minutes = MySqlClient
      .selectFrom(NBATables.minute_projection_2018_19, MinuteProjection.apply)
      .map(v => (v.Player, v))
      .toMap

    val schedule = MySqlClient
      .selectFrom(NBATables.league_results, LeagueSchedule.apply, "season = '2018'")
      .map(
        v =>
          (TeamMapper.teamInfo(v.HomeTeam.toInt, "2017-18").get.teamName,
           TeamMapper.teamInfo(v.AwayTeam.toInt, "2017-18").get.teamName))

    val teamAbbMap = TeamMapper.TeamMap.values.filter(_.season == "2017-18")
      .map(v => (v.teamAbbreviation.toLowerCase, v.teamName))
      .toMap


    val teams = MapJoin
      .leftOuterJoin(minutes, fourFactors)
      .map(v =>
        NetRatingContainer(
          v._1.Player,
          teamAbbMap.getOrElse(v._1.Team, v._1.Team),
          v._1.Min,
          v._2.map(_.LA_RAPM.doubleValue()).getOrElse(-1.5),
          v._2.map(_.LA_RAPM__Off.doubleValue()).getOrElse(-.45),
          v._2.map(_.LA_RAPM__Def.doubleValue()).getOrElse(-1.05),
          v._2.map(_.LA_RAPM__intercept)
        ))
      .groupBy(v => v.teamName)

    teams.foreach(println)

    val netRatings = teams
      .map(v => calculateWeightedNetRating(v._2))
      .map(v => (v.teamName, v))
      .toMap

    //netRatings.foreach(println)

    val teamWins = schedule
      .flatMap(v => {
        val homeRTG = netRatings(v._1)
        val awayRTG = netRatings(v._2)

        val pythag = round(sigmoid(homeRTG.rating, awayRTG.rating, 2))
        Seq((v._1, pythag), (v._2, 1 - pythag))
      })
      .groupBy(_._1)
      .map(v => (v._1, v._2.map(_._2).sum))

    MapJoin.join(netRatings, teamWins)
      .map(v => (v._1.teamName, v._1.rating, round(v._2), round(82.0-v._2)))
      .sortBy(_._3)
      .foreach(v => println(s"${v._1},${v._2},${v._3},${v._4}"))

    println()
    println()

    MapJoin.join(netRatings, teamWins)
      .map(v => (v._1.teamName, round(v._2)))
      .sortBy(_._2)
      .foreach(v => println(s"${v._1},${v._2}"))


  }

  def calculateWeightedNetRating(
      ratings: Seq[NetRatingContainer]): TeamProjectedNetRating = {
    val totalMin = ratings.map(_.minutes).sum.toDouble
    val netRating = ratings
      .map(v => v.lrapm * v.minutes.toDouble / totalMin)
      .sum * 5

    val intercept = ratings.flatMap(_.intercept).head

    val oRating = intercept + ratings
      .map(v => v.lrapmO * v.minutes.toDouble / totalMin)
      .sum * 5

    val dRating = intercept - ratings
      .map(v => v.lrapmD * v.minutes.toDouble / totalMin)
      .sum * 5

    val winPercent = ((netRating * 2.7) + 41.0)/82.0 // pythagWins(oRating, dRating, 14)
    //[(Points Differential)*2.7)+41]/82

    val wins = 82 * winPercent
    val losses = 82 - wins

    TeamProjectedNetRating(ratings.head.teamName,
                           round(netRating),
                           round(oRating),
                           round(dRating),
                           round(wins),
                           round(losses))
  }

  def round(double: Double): Double =
    math.round(double * 100.0) / 100.0

  def pythagWins(oRTG: Double, dRTG: Double, coef: Double): Double = {
    math.pow(oRTG, coef) / (math.pow(oRTG, coef) + math.pow(dRTG, coef))
  }

  def sigmoid(home: Double, away: Double, coef: Double): Double = {
    1.0 / (1 + math.exp(-0.21*(home-away)))
  }

}

final case class NetRatingContainer(playerName: String,
                                    teamName: String,
                                    minutes: Int,
                                    lrapm: Double,
                                    lrapmO: Double,
                                    lrapmD: Double,
                                    intercept: Option[Double])

final case class TeamProjectedNetRating(teamName: String,
                                        rating: Double,
                                        oRating: Double,
                                        dRating: Double,
                                        wins: Double,
                                        losses: Double)
