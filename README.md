# Russell

This is my personal project for downloading and analyzing nba data.


### Structure
Russell contains two main components and a scripts folder:
1. russell: A scala project for etl
2. visualization: a python project for analysis and data visualization
3. scripts: All scripts for running various parts of the codebase.


#### Common Commands
```
sh seasonStats.sh --season 2017-18 --run-all
sh playerStats.sh --season 2017-18 --shot-data
sh playersOnCourt.sh --season 2017-18 --run-all
sh run.sh com.rrdinsights.russell.investigation.shots.ShotsWithPlayers --season 2017-18
sh run.sh com.rrdinsights.russell.investigation.shots.ShotChartBuilder
sh run.sh com.rrdinsights.russell.investigation.shots.expectedshots.ScoreShotsCalculator --season 2017-18
sh run.sh com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedShotsPlayerCalculator --season 2017-18 -o -d -z
sh run.sh com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedShotsCalculator --season 2017-18 -o -d -z
sh run.sh com.rrdinsights.russell.investigation.shots.expectedshots.ExpectedShotsByGameCalculator --season 2017-18 -o -d -z
sh run.sh com.rrdinsights.russell.investigation.shots.shotmover.ShotMover --season 2017-18
sh run.sh com.rrdinsights.russell.investigation.shots.shotmover.ShotsSeen --season 2016-17
```