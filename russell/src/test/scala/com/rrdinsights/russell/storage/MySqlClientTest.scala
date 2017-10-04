package com.rrdinsights.russell.storage

import com.rrdinsights.russell.TestSpec
import com.rrdinsights.russell.storage.tables.NBATables
import com.rrdinsights.scalabrine.ScalabrineClient
import com.rrdinsights.scalabrine.endpoints.PlayByPlayEndpoint
import com.rrdinsights.scalabrine.parameters.GameIdParameter

final class MySqlClientTest extends TestSpec{
  test("create table") {
    //val pbp = ScalabrineClient.getPlayByPlay(PlayByPlayEndpoint(GameIdParameter.newParameterValue("0021600730"))).playByPlay.events

    println(MySqlClient.createTableStatement(NBATables.raw_play_by_play.name, NBATables.raw_play_by_play.columns))
  }
}