package com.rrdinsights.russell.storage.datamodel

import java.{lang => jl}

import com.rrdinsights.scalabrine.models.Coach

final case class RosterCoach(
                              primaryKey: String,
                              teamId: jl.Integer,
                              seasonPlayed: String,
                              coachId: String,
                              firstName: String,
                              lastName: String,
                              coachName: String,
                              coachCode: String,
                              isAssistant: jl.Boolean,
                              coachType: String,
                              school: String,
                              season: String,
                              dt: String)

object RosterCoach {
  def apply(coach: Coach, season: String, dt: String): RosterCoach =
    RosterCoach(
      s"${coach.coachId}_$season",
      coach.teamId,
      coach.season,
      coach.coachId,
      coach.firstName,
      coach.lastName,
      coach.coachName,
      coach.coachCode,
      coach.isAssistant,
      coach.coachType,
      coach.school,
      season,
      dt)
}