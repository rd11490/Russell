package com.rrdinsights.russell.storage.datamodel

import java.{lang => jl}

final case class RawInactivePlayers(
                                     primaryKey: String,
                                     gameId: String,
                                     playerId: jl.Integer,
                                     firstName: String,
                                     lastName: String,
                                     number: String,
                                     teamId: jl.Integer,
                                     teamCity: String,
                                     teamName: String,
                                     teamAbbreviation: String,
                                     dt: String,
                                     season: String)
