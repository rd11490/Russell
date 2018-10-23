package com.rrdinsights.russell.investigation.shots

import com.rrdinsights.russell.commandline.{CommandLineBase, SeasonOption}
import com.rrdinsights.russell.etl.application.{
  PlayersOnCourtDownloader,
  ShotChartDownloader
}
import com.rrdinsights.russell.investigation.GameDateMapper
import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.datamodel.{
  PlayersOnCourt,
  RawShotData,
  ShotWithPlayers
}
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.russell.utils.TimeUtils

object ShotsWithPlayers {

  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow
    val args = PlayerWithShotsArguments(strings)
    val where =
      args.seasonOpt.map(v => Seq(s"season = '$v'")).getOrElse(Seq.empty)
    val fullWhere = where :+ s"seasonType = '${args.seasonType}'"

    val shots = ShotChartDownloader.readShotData(fullWhere: _*)
    val players = PlayersOnCourtDownloader.readPlayersOnCourt(fullWhere: _*)

    writeShotsWithPlayers(joinShotsWithPlayers(shots, players, dt))

  }

  private def writeShotsWithPlayers(shots: Seq[ShotWithPlayers]): Unit = {
    MySqlClient.createTable(NBATables.lineup_shots)
    MySqlClient.insertInto(NBATables.lineup_shots, shots)
  }

  private def joinShotsWithPlayers(shots: Seq[RawShotData],
                                   players: Seq[PlayersOnCourt],
                                   dt: String): Seq[ShotWithPlayers] = {
    val shotMap = shots.map(v => ((v.gameId, v.eventNumber), v)).toMap
    val playersMap = players.map(v => ((v.gameId, v.eventNumber), v)).toMap

    shotMap
      .flatMap(v => playersMap.get(v._1).map(c => (v._2, c)))
      .toSeq
      .map(v => mergeShotDataWithPlayers(v._1, v._2, dt))
  }

  private def mergeShotDataWithPlayers(shot: RawShotData,
                                       players: PlayersOnCourt,
                                       dt: String): ShotWithPlayers = {

    val offenseTeamId = shot.teamId
    val defenseTeamId =
      if (shot.teamId == players.teamId1) players.teamId2 else players.teamId1

    val offensePlayer1 =
      if (shot.teamId == players.teamId1) players.team1player1Id
      else players.team2player1Id
    val offensePlayer2 =
      if (shot.teamId == players.teamId1) players.team1player2Id
      else players.team2player2Id
    val offensePlayer3 =
      if (shot.teamId == players.teamId1) players.team1player3Id
      else players.team2player3Id
    val offensePlayer4 =
      if (shot.teamId == players.teamId1) players.team1player4Id
      else players.team2player4Id
    val offensePlayer5 =
      if (shot.teamId == players.teamId1) players.team1player5Id
      else players.team2player5Id

    val defensePlayer1 =
      if (shot.teamId != players.teamId1) players.team1player1Id
      else players.team2player1Id
    val defensePlayer2 =
      if (shot.teamId != players.teamId1) players.team1player2Id
      else players.team2player2Id
    val defensePlayer3 =
      if (shot.teamId != players.teamId1) players.team1player3Id
      else players.team2player3Id
    val defensePlayer4 =
      if (shot.teamId != players.teamId1) players.team1player4Id
      else players.team2player4Id
    val defensePlayer5 =
      if (shot.teamId != players.teamId1) players.team1player5Id
      else players.team2player5Id

    ShotWithPlayers(
      primaryKey = s"${shot.gameId}_${shot.eventNumber}",
      gameId = shot.gameId,
      eventNumber = shot.eventNumber,
      shooter = shot.playerId,
      offenseTeamId = offenseTeamId,
      offensePlayer1Id = offensePlayer1,
      offensePlayer2Id = offensePlayer2,
      offensePlayer3Id = offensePlayer3,
      offensePlayer4Id = offensePlayer4,
      offensePlayer5Id = offensePlayer5,
      defenseTeamId = defenseTeamId,
      defensePlayer1Id = defensePlayer1,
      defensePlayer2Id = defensePlayer2,
      defensePlayer3Id = defensePlayer3,
      defensePlayer4Id = defensePlayer4,
      defensePlayer5Id = defensePlayer5,
      shotDistance = shot.shotDistance,
      xCoordinate = shot.xCoordinate,
      yCoordinate = shot.yCoordinate,
      shotZone = ShotZone.findShotZone(shot).toString,
      shotAttemptedFlag = shot.shotAttemptedFlag,
      shotMadeFlag = shot.shotMadeFlag,
      shotValue = shot.shotValue,
      period = shot.period,
      minutesRemaining = shot.minutesRemaining,
      secondsRemaining = shot.secondsRemaining,
      gameDate = GameDateMapper.gameDate(shot.gameId).get.gameDateInMillis,
      season = shot.season,
      seasonType = players.seasonType,
      dt = dt
    )
  }

}

private final class PlayerWithShotsArguments private (args: Array[String])
    extends CommandLineBase(args, "Player Stats")
    with SeasonOption

private object PlayerWithShotsArguments {

  def apply(args: Array[String]): PlayerWithShotsArguments =
    new PlayerWithShotsArguments(args)

}
