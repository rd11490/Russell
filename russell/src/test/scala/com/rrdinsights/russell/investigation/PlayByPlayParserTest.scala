package com.rrdinsights.russell.investigation
import com.rrdinsights.russell.TestSpec
import com.rrdinsights.russell.storage.datamodel.{PlayByPlayEventMessageType, RawPlayByPlayEvent}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.{lang => jl}
@RunWith(classOf[JUnitRunner])
final class PlayByPlayParserTest extends TestSpec {
  test("sort Play by Play") {
    val playByPlay = Seq(
      buildRawPlayByPlayEvent(1, PlayByPlayEventMessageType.StartOfPeriod),
      buildRawPlayByPlayEvent(2, PlayByPlayEventMessageType.Miss),
      buildRawPlayByPlayEvent(3, PlayByPlayEventMessageType.Foul),
      buildRawPlayByPlayEvent(4, PlayByPlayEventMessageType.FreeThrow),
      buildRawPlayByPlayEvent(5, PlayByPlayEventMessageType.Substitution),
      buildRawPlayByPlayEvent(6, PlayByPlayEventMessageType.FreeThrow))

    val sortedPlayByPlay = PlayByPlayParser.properlySortPlayByPlay(playByPlay)

    assert(sortedPlayByPlay.map(v => PlayByPlayEventMessageType.valueOf(v.playType)) === Seq(
      PlayByPlayEventMessageType.StartOfPeriod,
      PlayByPlayEventMessageType.Miss,
      PlayByPlayEventMessageType.Foul,
      PlayByPlayEventMessageType.FreeThrow,
      PlayByPlayEventMessageType.FreeThrow,
      PlayByPlayEventMessageType.Substitution))
  }

  test("sort Play by Play2") {
    val playByPlay = Seq(
      buildRawPlayByPlayEvent(1, PlayByPlayEventMessageType.StartOfPeriod),
      buildRawPlayByPlayEvent(2, PlayByPlayEventMessageType.Miss),
      buildRawPlayByPlayEvent(3, PlayByPlayEventMessageType.Foul),
      buildRawPlayByPlayEvent(4, PlayByPlayEventMessageType.FreeThrow),
      buildRawPlayByPlayEvent(5, PlayByPlayEventMessageType.Substitution),
      buildRawPlayByPlayEvent(6, PlayByPlayEventMessageType.FreeThrow),
      buildRawPlayByPlayEvent(7, PlayByPlayEventMessageType.FreeThrow))

    val sortedPlayByPlay = PlayByPlayParser.properlySortPlayByPlay(playByPlay)

    assert(sortedPlayByPlay.map(v => PlayByPlayEventMessageType.valueOf(v.playType)) === Seq(
      PlayByPlayEventMessageType.StartOfPeriod,
      PlayByPlayEventMessageType.Miss,
      PlayByPlayEventMessageType.Foul,
      PlayByPlayEventMessageType.FreeThrow,
      PlayByPlayEventMessageType.FreeThrow,
      PlayByPlayEventMessageType.FreeThrow,
      PlayByPlayEventMessageType.Substitution))
  }
  test("sort Play by Play3") {
    val playByPlay = Seq(
      buildRawPlayByPlayEvent(1, PlayByPlayEventMessageType.StartOfPeriod),
      buildRawPlayByPlayEvent(2, PlayByPlayEventMessageType.Miss),
      buildRawPlayByPlayEvent(3, PlayByPlayEventMessageType.Substitution),
      buildRawPlayByPlayEvent(4, PlayByPlayEventMessageType.Foul),
      buildRawPlayByPlayEvent(5, PlayByPlayEventMessageType.FreeThrow),
      buildRawPlayByPlayEvent(6, PlayByPlayEventMessageType.FreeThrow))

    val sortedPlayByPlay = PlayByPlayParser.properlySortPlayByPlay(playByPlay)

    assert(sortedPlayByPlay.map(v => PlayByPlayEventMessageType.valueOf(v.playType)) === Seq(
      PlayByPlayEventMessageType.StartOfPeriod,
      PlayByPlayEventMessageType.Miss,
      PlayByPlayEventMessageType.Substitution,
      PlayByPlayEventMessageType.Foul,
      PlayByPlayEventMessageType.FreeThrow,
      PlayByPlayEventMessageType.FreeThrow))
  }


  private def buildRawPlayByPlayEvent(eventNumber: jl.Integer, playType: PlayByPlayEventMessageType): RawPlayByPlayEvent =
    RawPlayByPlayEvent(
      s"1_$eventNumber",
      "1",
      eventNumber,
      playType.toString,
      null,
      null,
      null,
      null,
      null,
      null,
      null,

      null,
      null,

      null,
      null,
      null,
      null,
      null,
      null,
      null,

      null,
      null,
      null,
      null,
      null,
      null,
      null,

      null,
      null,
      null,
      null,
      null,
      null,
      null,

      null,
      null)
}
