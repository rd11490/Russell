package com.rrdinsights.russell.storage.datamodel

import java.sql.ResultSet
import java.{lang => jl}

final case class RealAdjustedFourFactors(playerId: Integer,
                                         playerName: String,
                                         LA_RAPM: jl.Double,
                                         LA_RAPM_Rank: jl.Double,
                                         LA_RAPM__Def: jl.Double,
                                         LA_RAPM__Def_Rank: jl.Double,
                                         LA_RAPM__Off: jl.Double,
                                         LA_RAPM__Off_Rank: jl.Double,
                                         LA_RAPM__intercept: jl.Double,
                                         RA_EFG: jl.Double,
                                         RA_EFG_Rank: jl.Double,
                                         RA_EFG__Def: jl.Double,
                                         RA_EFG__Def_Rank: jl.Double,
                                         RA_EFG__Off: jl.Double,
                                         RA_EFG__Off_Rank: jl.Double,
                                         RA_EFG__intercept: jl.Double,
                                         RA_FTR: jl.Double,
                                         RA_FTR_Rank: jl.Double,
                                         RA_FTR__Def: jl.Double,
                                         RA_FTR__Def_Rank: jl.Double,
                                         RA_FTR__Off: jl.Double,
                                         RA_FTR__Off_Rank: jl.Double,
                                         RA_FTR__intercept: jl.Double,
                                         RA_ORBD: jl.Double,
                                         RA_ORBD_Rank: jl.Double,
                                         RA_ORBD__Def: jl.Double,
                                         RA_ORBD__Def_Rank: jl.Double,
                                         RA_ORBD__Off: jl.Double,
                                         RA_ORBD__Off_Rank: jl.Double,
                                         RA_ORBD__intercept: jl.Double,
                                         RA_TOV: jl.Double,
                                         RA_TOV_Rank: jl.Double,
                                         RA_TOV__Def: jl.Double,
                                         RA_TOV__Def_Rank: jl.Double,
                                         RA_TOV__Off: jl.Double,
                                         RA_TOV__Off_Rank: jl.Double,
                                         RA_TOV__intercept: jl.Double,
                                         RAPM: jl.Double,
                                         RAPM_Rank: jl.Double,
                                         RAPM__Def: jl.Double,
                                         RAPM__Def_Rank: jl.Double,
                                         RAPM__Off: jl.Double,
                                         RAPM__Off_Rank: jl.Double,
                                         RAPM__intercept: jl.Double,
                                         season: String)

object RealAdjustedFourFactors extends ResultSetMapper {

  def apply(resultSet: ResultSet): RealAdjustedFourFactors =
    RealAdjustedFourFactors(
      playerId = getInt(resultSet, 0),
      playerName = getString(resultSet, 1),
      LA_RAPM = getDouble(resultSet, 2),
      LA_RAPM_Rank = getDouble(resultSet, 3),
      LA_RAPM__Def = getDouble(resultSet, 4),
      LA_RAPM__Def_Rank = getDouble(resultSet, 5),
      LA_RAPM__Off = getDouble(resultSet, 6),
      LA_RAPM__Off_Rank = getDouble(resultSet, 7),
      LA_RAPM__intercept = getDouble(resultSet, 8),
      RA_EFG = getDouble(resultSet, 9),
      RA_EFG_Rank = getDouble(resultSet, 10),
      RA_EFG__Def = getDouble(resultSet, 11),
      RA_EFG__Def_Rank = getDouble(resultSet, 12),
      RA_EFG__Off = getDouble(resultSet, 13),
      RA_EFG__Off_Rank = getDouble(resultSet, 14),
      RA_EFG__intercept = getDouble(resultSet, 15),
      RA_FTR = getDouble(resultSet, 16),
      RA_FTR_Rank = getDouble(resultSet, 17),
      RA_FTR__Def = getDouble(resultSet, 18),
      RA_FTR__Def_Rank = getDouble(resultSet, 19),
      RA_FTR__Off = getDouble(resultSet, 20),
      RA_FTR__Off_Rank = getDouble(resultSet, 21),
      RA_FTR__intercept = getDouble(resultSet, 22),
      RA_ORBD = getDouble(resultSet, 23),
      RA_ORBD_Rank = getDouble(resultSet, 24),
      RA_ORBD__Def = getDouble(resultSet, 25),
      RA_ORBD__Def_Rank = getDouble(resultSet, 26),
      RA_ORBD__Off = getDouble(resultSet, 27),
      RA_ORBD__Off_Rank = getDouble(resultSet, 28),
      RA_ORBD__intercept = getDouble(resultSet, 29),
      RA_TOV = getDouble(resultSet, 30),
      RA_TOV_Rank = getDouble(resultSet, 31),
      RA_TOV__Def = getDouble(resultSet, 32),
      RA_TOV__Def_Rank = getDouble(resultSet, 33),
      RA_TOV__Off = getDouble(resultSet, 34),
      RA_TOV__Off_Rank = getDouble(resultSet, 35),
      RA_TOV__intercept = getDouble(resultSet, 36),
      RAPM = getDouble(resultSet, 37),
      RAPM_Rank = getDouble(resultSet, 38),
      RAPM__Def = getDouble(resultSet, 39),
      RAPM__Def_Rank = getDouble(resultSet, 40),
      RAPM__Off = getDouble(resultSet, 41),
      RAPM__Off_Rank = getDouble(resultSet, 42),
      RAPM__intercept = getDouble(resultSet, 43),
      season = getString(resultSet, 44)
    )
}
