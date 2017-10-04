package com.rrdinsights.russell.storage.datamodel

import java.{lang => jl}

final case class PlayByPlayLineup(
                                   primaryKey: String,
                                   gameId: String,
                                   eventNumber: jl.Integer,
                                   homePlayer1: String,
                                   homePlayer2: String,
                                   homePlayer3: String,
                                   homePlayer4: String,
                                   homePlayer5: String,
                                   awayPlayer1: String,
                                   awayPlayer2: String,
                                   awayPlayer3: String,
                                   awayPlayer4: String,
                                   awayPlayer5: String,
                                   season: String,
                                   dt: String)