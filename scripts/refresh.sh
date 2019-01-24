#!/bin/bash

. "$(dirname "${BASH_SOURCE[0]}")/commons.sh"

echo $JAR

sh seasonStats.sh "$@" --run-all
sh playerStats.sh "$@" --shot-data
sh playersOnCourt.sh "$@" --run-all
sh run.sh com.rrdinsights.russell.etl.application.GameDateMapBuilder
sh run.sh com.rrdinsights.russell.etl.application.PlayerIdMapBuilder
sh run.sh com.rrdinsights.russell.etl.application.TeamIdMapBuilder
sh run.sh com.rrdinsights.russell.investigation.shots.ShotsWithPlayers "$@"

sh run.sh com.rrdinsights.russell.etl.driver.ShotSitePutter "$@" --run-all
 
