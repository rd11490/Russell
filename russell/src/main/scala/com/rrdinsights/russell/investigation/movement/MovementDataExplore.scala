package com.rrdinsights.russell.investigation.movement

import java.sql.ResultSet
import java.{lang => jl}

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.ResultSetMapper
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.MapJoin

object MovementDataExplore {

  def main(strings: Array[String]): Unit = {

    val events = MySqlClient.selectFrom(NBATables.movement_data, EventRow.apply, "game_id = '0021500513'")

    val eventDetails = events
      .groupBy(v => (v.player_id, v.game_id, v.quarter, v.event_id))
      .flatMap(v => calculateMovementData(v._2))

    println(s"Events: ${eventDetails.size}")

    eventDetails.map(_.player_id).groupBy(identity).keys.foreach(println)

    val ballEventDetails = eventDetails
      .filter(_.player_id == -1)
      .map(v => (MapKey(v.event_id, v.game_clock, v.game_id, v.quarter), v))
      .toMap

    println(s"Ball Events: ${ballEventDetails.size}")

    val playerEventDetails = eventDetails
      .filter(_.player_id != -1)
      .map(v => (MapKey(v.event_id, v.game_clock, v.game_id, v.quarter), v))
      .toSeq

    println(s"Player Events: ${playerEventDetails.size}")


    val playersWithBall = MapJoin.joinSeq(playerEventDetails, ballEventDetails)
      .flatMap(v => toPlayerWithBall(v._1, v._2))

    println(playersWithBall.size)

    MySqlClient.createTable(NBATables.player_with_ball)
    MySqlClient.insertInto(NBATables.player_with_ball, playersWithBall)

  }

  private def toPlayerWithBall(player: EventDetailRow, ball: EventDetailRow): Option[PlayerWithBall] = {
    if (ball != null) {
      Some(PlayerWithBall(
        s"${player.game_id}_${player.player_id}_${player.game_clock}_${player.event_id}",
        player.team_id,
        player.player_id,
        player.x_loc,
        player.y_loc,
        player.x_unit,
        player.y_unit,
        player.x_velocity,
        player.x_velocity,
        player.speed,
        player.x_acceleration,
        player.y_acceleration,
        player.acceleration,

        ball.x_loc,
        ball.y_loc,
        ball.x_unit,
        ball.y_unit,
        ball.x_velocity,
        ball.x_velocity,
        ball.speed,
        ball.x_acceleration,
        ball.y_acceleration,
        ball.acceleration,

        player.game_clock,
        player.shot_clock,
        player.quarter,
        player.game_id,
        player.event_id))
    } else {
      None
    }

  }

  private def calculateDistance(xLoc1: Double, yLoc1: Double, xLoc2: Double, yLoc2: Double): Double =
    math.sqrt(math.pow(xLoc2 - xLoc1, 2) + math.pow(yLoc2 - yLoc1, 2))

  private def calculateMovementData(events: Seq[EventRow]): Seq[EventDetailRow] = {
    events.sortBy(_.game_clock).reverse.sliding(3).map(v => calculateSingleEvent(v)).toSeq
  }

  private def calculateDerivative(v1: Double, v2: Double, t1: Double, t2: Double): Double =
    (v2 - v1) / 0.04


  private def calculateChange(v1: Double, v2: Double, t1: Double, t2: Double): Double =
    math.abs(calculateDerivative(v1, v2, t1, t2))

  private def calculateDerivative(x1: Double, x2: Double, y1: Double, y2: Double, t1: Double, t2: Double): Double =
    calculateDistance(x1, x2, y1, y2) / 0.04

  private def calculateSpeed(first: EventRow, second: EventRow): Double =
    calculateDerivative(first.x_loc, second.x_loc, first.y_loc, second.y_loc, first.game_clock, second.game_clock)

  private def calculateAcceleration(first: EventRow, second: EventRow, third: EventRow): Double = {
    val vel21 = calculateSpeed(first, second)
    val vel32 = calculateSpeed(second, third)
    calculateDerivative(vel21, vel32, first.game_clock, second.game_clock)
  }

  private def calculateSingleEvent(events: Seq[EventRow]): EventDetailRow = {
    val first = events.head
    val second = events(1)
    val third = events(2)

    val velocity32x = calculateChange(second.x_loc, third.x_loc, second.game_clock, third.game_clock)
    val velocity21x = calculateChange(first.x_loc, second.x_loc, first.game_clock, second.game_clock)
    val velocityx = (velocity32x + velocity21x) / 2.0
    val accelx = calculateChange(velocity21x, velocity32x, first.game_clock, second.game_clock)

    val velocity32y = calculateChange(second.y_loc, third.y_loc, second.game_clock, third.game_clock)
    val velocity21y = calculateChange(first.y_loc, second.y_loc, first.game_clock, second.game_clock)
    val velocityy = (velocity32y + velocity21y) / 2.0
    val accely = calculateChange(velocity21y, velocity32y, first.game_clock, second.game_clock)

    val dist = calculateDistance(first.x_loc, first.y_loc, third.x_loc, third.y_loc)
    val unitx = if (dist != 0.0) (third.x_loc - first.x_loc) / dist else 0.0
    val unity = if (dist != 0.0) (third.y_loc - first.y_loc) / dist else 0.0

    val speed = calculateSpeed(first, third)
    val accel = calculateAcceleration(first, second, third)

    EventDetailRow(
      second.team_id,
      second.player_id,
      second.x_loc,
      second.y_loc,
      unitx,
      unity,
      velocityx,
      velocityy,
      speed,
      accelx,
      accely,
      accel,
      second.game_clock,
      second.shot_clock,
      second.quarter,
      second.game_id,
      second.event_id)
  }
}

private final case class MapKey(event_id: Int, game_clock: jl.Double, game_id: String, quarter: Int)


final case class EventRow(
                           team_id: Int,
                           player_id: Int,
                           x_loc: jl.Double,
                           y_loc: jl.Double,
                           radius: jl.Double,
                           game_clock: jl.Double,
                           shot_clock: jl.Double,
                           quarter: Int,
                           game_id: String,
                           event_id: Int)

object EventRow extends ResultSetMapper {
  def apply(resultSet: ResultSet): EventRow = {
    EventRow(getInt(resultSet, 0),
      getInt(resultSet, 1),
      getDouble(resultSet, 2),
      getDouble(resultSet, 3),
      getDouble(resultSet, 4),
      getDouble(resultSet, 5),
      getDouble(resultSet, 6),
      getInt(resultSet, 7),
      getString(resultSet, 8),
      getInt(resultSet, 9))
  }
}

private final case class EventDetailRow(
                                         team_id: Int,
                                         player_id: Int,
                                         x_loc: jl.Double,
                                         y_loc: jl.Double,
                                         x_unit: jl.Double,
                                         y_unit: jl.Double,
                                         x_velocity: jl.Double,
                                         y_velocity: jl.Double,
                                         speed: jl.Double,
                                         x_acceleration: jl.Double,
                                         y_acceleration: jl.Double,
                                         acceleration: jl.Double,
                                         game_clock: jl.Double,
                                         shot_clock: jl.Double,
                                         quarter: Int,
                                         game_id: String,
                                         event_id: Int)

final case class PlayerWithBall(
                                 primaryKey: String,
                                 team_id: Int,
                                 player_id: Int,

                                 x_loc: jl.Double,
                                 y_loc: jl.Double,
                                 x_unit: jl.Double,
                                 y_unit: jl.Double,
                                 x_velocity: jl.Double,
                                 y_velocity: jl.Double,
                                 speed: jl.Double,
                                 x_acceleration: jl.Double,
                                 y_acceleration: jl.Double,
                                 acceleration: jl.Double,

                                 ball_x_loc: jl.Double,
                                 ball_y_loc: jl.Double,
                                 ball_x_unit: jl.Double,
                                 ball_y_unit: jl.Double,
                                 ball_x_velocity: jl.Double,
                                 ball_y_velocity: jl.Double,
                                 ball_speed: jl.Double,
                                 ball_x_acceleration: jl.Double,
                                 ball_y_acceleration: jl.Double,
                                 ball_acceleration: jl.Double,

                                 game_clock: jl.Double,
                                 shot_clock: jl.Double,
                                 quarter: Int,
                                 game_id: String,
                                 event_id: Int)