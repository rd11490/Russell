package com.rrdinsights.russell.investigation.shots

import com.rrdinsights.russell.commandline.{CommandLineBase, SeasonOption}
import com.rrdinsights.russell.etl.application.{PlayersOnCourtDownloader, ShotChartDownloader}
import com.rrdinsights.russell.storage.datamodel.{DataModelUtils, PlayersOnCourt, RawShotData, ShotWithPlayers}
import com.rrdinsights.russell.utils.TimeUtils

import com.rrdinsights.russell.storage.MySqlClient
import com.rrdinsights.russell.storage.tables.NBATables

object ShotsWithPlayers {

  def main(strings: Array[String]): Unit = {
    val dt = TimeUtils.dtNow
    val args = PlayerWithShotsArguments(strings)
    val where = args.seasonOpt.map(v => Seq(s"season = '$v'")).getOrElse(Seq.empty)

    val shots = ShotChartDownloader.readShotData(where: _*)
    val players = PlayersOnCourtDownloader.readPlayersOnCourt(where: _*)

    writeShotsWithPlayers(joinShotsWithPlayers(shots, players, dt))

  }

  private def writeShotsWithPlayers(shots: Seq[ShotWithPlayers]): Unit = {
    MySqlClient.createTable(NBATables.lineup_shots)
    MySqlClient.insertInto(NBATables.lineup_shots, shots)
  }

  private def joinShotsWithPlayers(shots: Seq[RawShotData], players: Seq[PlayersOnCourt], dt: String): Seq[ShotWithPlayers] = {
    val shotMap = shots.map(v => ((v.gameId, v.eventNumber), v)).toMap
    val playersMap = players.map(v=> ((v.gameId, v.eventNumber), v)).toMap

    shotMap.flatMap(v => playersMap.get(v._1).map(c => (v._2, c)))
      .toSeq
      .map(v => mergeShotDataWithPlayers(v._1, v._2, dt))
  }

  private def mergeShotDataWithPlayers(shot: RawShotData, players: PlayersOnCourt, dt: String): ShotWithPlayers = {

    val offenseTeamId = shot.teamId
    val defenseTeamId = if (shot.teamId == players.teamId1) players.teamId2 else players.teamId1

    val offensePlayer1 = if (shot.teamId == players.teamId1) players.team1player1Id else players.team2player1Id
    val offensePlayer2 = if (shot.teamId == players.teamId1) players.team1player2Id else players.team2player2Id
    val offensePlayer3 = if (shot.teamId == players.teamId1) players.team1player3Id else players.team2player3Id
    val offensePlayer4 = if (shot.teamId == players.teamId1) players.team1player4Id else players.team2player4Id
    val offensePlayer5 = if (shot.teamId == players.teamId1) players.team1player5Id else players.team2player5Id

    val defensePlayer1 = if (shot.teamId != players.teamId1) players.team1player1Id else players.team2player1Id
    val defensePlayer2 = if (shot.teamId != players.teamId1) players.team1player2Id else players.team2player2Id
    val defensePlayer3 = if (shot.teamId != players.teamId1) players.team1player3Id else players.team2player3Id
    val defensePlayer4 = if (shot.teamId != players.teamId1) players.team1player4Id else players.team2player4Id
    val defensePlayer5 = if (shot.teamId != players.teamId1) players.team1player5Id else players.team2player5Id

    ShotWithPlayers(
      s"${shot.gameId}_${shot.eventNumber}",
      shot.gameId,
      shot.eventNumber,
      shot.playerId,
      offenseTeamId,
      offensePlayer1,
      offensePlayer2,
      offensePlayer3,
      offensePlayer4,
      offensePlayer5,
      defenseTeamId,
      defensePlayer1,
      defensePlayer2,
      defensePlayer3,
      defensePlayer4,
      defensePlayer5,
      shot.shotDistance,
      shot.xCoordinate,
      shot.yCoordinate,
      shot.shotAttemptedFlag,
      shot.shotMadeFlag,
      shot.shotValue,
      shot.period,
      shot.minutesRemaining,
      shot.secondsRemaining,
      DataModelUtils.gameIdToSeason(shot.gameId),
      dt)
  }

}

private final class PlayerWithShotsArguments private(args: Array[String])
  extends CommandLineBase(args, "Player Stats") with SeasonOption

private object PlayerWithShotsArguments {

  def apply(args: Array[String]): PlayerWithShotsArguments = new PlayerWithShotsArguments(args)

}
