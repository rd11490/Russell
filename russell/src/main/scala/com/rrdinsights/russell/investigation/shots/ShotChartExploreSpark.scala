package com.rrdinsights.russell.investigation.shots

import java.time.Instant

import com.rrdinsights.russell.spark.job.LocalJobRunner
import com.rrdinsights.russell.storage.datamodel.{PlayersOnCourt, RawShotData, RosterPlayer}
import com.rrdinsights.russell.storage.tables.NBATables
import org.apache.spark.sql.{Column, SparkSession, functions}
import java.{lang => jl}

object ShotChartExploreSpark extends LocalJobRunner {

  import com.rrdinsights.russell.spark.sql.SparkOpsImplicits._

  override def runSpark2(strings: Array[String])(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val dt = Instant.now

    val where = "season = '2016-17'"

    val players = readTable(NBATables.roster_player)
      .where(where)
      .as[RosterPlayer]
      .map(_.playerId)
      .collect()

    val shots = readTable(NBATables.raw_shot_data)
      .as[RawShotData]
      .filter(v => players.contains(v.playerId))

    val shotHistograms = shots
      .groupByKey(_.playerId)
      .flatMapGroups((p, shots) => ShotHistogram.calculate(shots.toSeq))
      .map(v => PlayerShotChartSection.apply(v._1, v._2, ))

    val shotsSeason = readTable(NBATables.raw_shot_data)
        .where(where)
        .as[RawShotData]


    val playersOnCourt = readTable(NBATables.players_on_court)
      .where(where)
      .as[PlayersOnCourt]

    val out = shots.leftOuterJoinWithUsingDS(playersOnCourt, LocalColumns.gameId, LocalColumns.eventNumber)


  }

}

private case class ShowWithPlayers(
                                    gameId: String,
                                    eventNumber: jl.Integer,
                                    shooterTeam: jl.Integer,
                                    shooter: jl.Integer,

                                  )

/*
shotZoneBasic: String,
shotZoneArea: String,
shotZoneRange: String,
shotType: String,
shotDistance: jl.Integer,
xCoordinate: jl.Integer,
yCoordinate: jl.Integer,
shotAttemptedFlag: jl.Integer,
shotMadeFlag: jl.Integer,
gameDate: String,
homeTeam: String,
awayTeam: String,
season: String,
dt: String

 */

private object LocalColumns {
  val gameId: Column = functions.col("gameId")
  val eventNumber: Column = functions.col("eventNumber")
}