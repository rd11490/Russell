#!/bin/bash

. "$(dirname "${BASH_SOURCE[0]}")/commons.sh"

echo $JAR

sh seasonStats.sh "$@" --run-all --force
sh playerStats.sh "$@" --run-all --force
sh playersOnCourt.sh "$@" --run-all
sh run.sh com.rrdinsights.russell.etl.application.GameDateMapBuilder
sh run.sh com.rrdinsights.russell.etl.application.PlayerIdMapBuilder
sh run.sh com.rrdinsights.russell.etl.application.TeamIdMapBuilder
sh run.sh com.rrdinsights.russell.investigation.shots.ShotsWithPlayers "$@"

 
sh run.sh com.rrdinsights.russell.investigation.shots.ShotChartBuilder
sh run.sh com.rrdinsights.russell.investigation.shots.expectedshots.ScoreShotsCalculator "$@"
sh run.sh com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedShotsPlayerCalculator "$@" -o -d
sh run.sh com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedShotsCalculator "$@" -o -d
sh run.sh com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedShotsByGameCalculator "$@" -o -d
sh run.sh com.rrdinsights.russell.investigation.shots.shotmover.ShotDeterrence "$@"
sh run.sh com.rrdinsights.russell.investigation.shots.shotmover.ShotsSeen "$@"
sh run.sh com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedShotsPlayerOnOffCalculator "$@" -o -d


sh run.sh com.rrdinsights.russell.investigation.playbyplay.PlayByPlayLineupJoiner "$@"
sh run.sh com.rrdinsights.russell.investigation.playbyplay.luckadjusted.LuckAdjustedStints "$@"
sh run.sh com.rrdinsights.russell.investigation.playbyplay.luckadjusted.LuckAdjustedPossessions "$@"

