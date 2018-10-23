#!/bin/bash

. "$(dirname "${BASH_SOURCE[0]}")/commons.sh"

echo $JAR

sh seasonStats.sh --season "$@" --run-all
sh playerStats.sh --season "$@" --run-all
sh playersOnCourt.sh --season "$@" --run-all
sh run.sh com.rrdinsights.russell.etl.application.GameDateMapBuilder
sh run.sh com.rrdinsights.russell.etl.application.PlayerIdMapBuilder
sh run.sh com.rrdinsights.russell.etl.application.TeamIdMapBuilder
sh run.sh com.rrdinsights.russell.investigation.shots.ShotsWithPlayers --season "$@"

sh run.sh com.rrdinsights.russell.etl.driver.ShotSitePutter --season "$@" --run-all
 
