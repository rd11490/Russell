package com.rrdinsights.russell.investigation.playbyplay

sealed trait FoulType {
  def value: Int

  def technicalFreeThrow: Boolean

  def freeThrow: Boolean
}
/*
FOUL TYPES
 *1 - Personal
 *2 - Shooting
 *3 - Loose Ball
 *4 - Offensive
 *5 - Inbound foul (1 FTA)
 *6 - Away from play
8 - Punch foul (Technical)
 *9 - Clear Path
 *10 - Double Foul
11 - Technical
12 - Non-Unsportsmanlike (Technical)
13 - Hanging (Technical)
 *14 - Flagrant 1
 *15 - Flagrant 2
16 - Double Technical
 *17 - Defensive 3 seconds (Technical)
18 - Delay of game
19 - Taunting (Technical)
25 - Excess Timeout (Technical)
 *26 - Charge
 *27 - Personal Block
 *28 - Personal Take
 *29 - Shooting Block
30 - Too many players (Technical)
 */

object FoulType {

  private val foulTypes: Seq[FoulType] = Seq(
    Personal,
    Shooting,
    LooseBall,
    Offensive,
    Inbound,
    AwayFromPlay,
    Punch,
    ClearPath,
    DoubleFoul,
    Technical,
    Unsportsmanlike,
    Hanging,
    Flagrant1,
    Flagrant2,
    DoubleTechnical,
    Defensive3Seconds,
    DelayOfGame,
    Taunting,
    ExcessiveTimeout,
    Charge,
    PersonalBlock,
    PersonalTake,
    ShootingBlock,
    TooManyPlayers
  )

  private val offensiveFouls: Seq[FoulType] = Seq(Offensive, Charge)

  def valueOf(i: Int): FoulType =
    foulTypes.find(_.value == i).getOrElse(Unknown)

  def isOffensive(i: Int): Boolean =
    offensiveFouls.contains(valueOf(i))

  case object Personal extends FoulType {
    override val value: Int = 1
    override val technicalFreeThrow: Boolean = false
    override val freeThrow: Boolean = false
  }
  case object Shooting extends FoulType {
    override val value: Int = 2
    override val technicalFreeThrow: Boolean = false
    override val freeThrow: Boolean = true
  }
  case object LooseBall extends FoulType {
    override val value: Int = 3
    override val technicalFreeThrow: Boolean = false
    override val freeThrow: Boolean = false
  }
  case object Offensive extends FoulType {
    override val value: Int = 4
    override val technicalFreeThrow: Boolean = false
    override val freeThrow: Boolean = false
  }
  case object Inbound extends FoulType {
    override val value: Int = 5
    override val technicalFreeThrow: Boolean = false
    override val freeThrow: Boolean = true
  }
  case object AwayFromPlay extends FoulType {
    override val value: Int = 6
    override val technicalFreeThrow: Boolean = false
    override val freeThrow: Boolean = false
  }
  case object Punch extends FoulType {
    override val value: Int = 8
    override val technicalFreeThrow: Boolean = false
    override val freeThrow: Boolean = false
  }
  case object ClearPath extends FoulType {
    override val value: Int = 9
    override val technicalFreeThrow: Boolean = false
    override val freeThrow: Boolean = true
  }
  case object DoubleFoul extends FoulType {
    override val value: Int = 10
    override val technicalFreeThrow: Boolean = false
    override val freeThrow: Boolean = false
  }
  case object Technical extends FoulType {
    override val value: Int = 11
    override val technicalFreeThrow: Boolean = true
    override val freeThrow: Boolean = true
  }
  case object Unsportsmanlike extends FoulType {
    override val value: Int = 12
    override val technicalFreeThrow: Boolean = false
    override val freeThrow: Boolean = false
  }
  case object Hanging extends FoulType {
    override val value: Int = 13
    override val technicalFreeThrow: Boolean = true
    override val freeThrow: Boolean = true
  }
  case object Flagrant1 extends FoulType {
    override val value: Int = 14
    override val technicalFreeThrow: Boolean = true
    override val freeThrow: Boolean = true
  }
  case object Flagrant2 extends FoulType {
    override val value: Int = 15
    override val technicalFreeThrow: Boolean = true
    override val freeThrow: Boolean = true
  }
  case object DoubleTechnical extends FoulType {
    override val value: Int = 16
    override val technicalFreeThrow: Boolean = false
    override val freeThrow: Boolean = false
  }
  case object Defensive3Seconds extends FoulType {
    override val value: Int = 17
    override val technicalFreeThrow: Boolean = true
    override val freeThrow: Boolean = true
  }
  case object DelayOfGame extends FoulType {
    override val value: Int = 18
    override val technicalFreeThrow: Boolean = false
    override val freeThrow: Boolean = false
  }
  case object Taunting extends FoulType {
    override val value: Int = 19
    override val technicalFreeThrow: Boolean = true
    override val freeThrow: Boolean = true
  }
  case object ExcessiveTimeout extends FoulType {
    override val value: Int = 25
    override val technicalFreeThrow: Boolean = true
    override val freeThrow: Boolean = true
  }
  case object Charge extends FoulType {
    override val value: Int = 26
    override val technicalFreeThrow: Boolean = false
    override val freeThrow: Boolean = false
  }
  case object PersonalBlock extends FoulType {
    override val value: Int = 27
    override val technicalFreeThrow: Boolean = false
    override val freeThrow: Boolean = false
  }
  case object PersonalTake extends FoulType {
    override val value: Int = 28
    override val technicalFreeThrow: Boolean = false
    override val freeThrow: Boolean = false
  }
  case object ShootingBlock extends FoulType {
    override val value: Int = 29
    override val technicalFreeThrow: Boolean = false
    override val freeThrow: Boolean = true
  }
  case object TooManyPlayers extends FoulType {
    override val value: Int = 30
    override val technicalFreeThrow: Boolean = true
    override val freeThrow: Boolean = true
  }

  case object Unknown extends FoulType {
    override val value: Int = -1
    override val technicalFreeThrow: Boolean = false
    override val freeThrow: Boolean = false
  }

}

sealed trait FreeThrowType {
  def value: Int
}

object FreeThrowType {
  private val freeThrowTypes: Seq[FreeThrowType] =
    Seq(OneOfOne,
        OneOfTwo,
        TwoOfTwo,
        OneOfThree,
        TwoOfThree,
        ThreeOfThree,
        Technical)

  def valueOf(i: Int): FreeThrowType =
    freeThrowTypes.find(_.value == i).getOrElse(Unknown)

  case object OneOfOne extends FreeThrowType {
    override val value: Int = 10
  }
  case object OneOfTwo extends FreeThrowType {
    override val value: Int = 11
  }
  case object TwoOfTwo extends FreeThrowType {
    override val value: Int = 12
  }
  case object OneOfThree extends FreeThrowType {
    override val value: Int = 13
  }
  case object TwoOfThree extends FreeThrowType {
    override val value: Int = 14
  }
  case object ThreeOfThree extends FreeThrowType {
    override val value: Int = 15
  }
  case object Technical extends FreeThrowType {
    override val value: Int = 16
  }
  case object Unknown extends FreeThrowType {
    override val value: Int = -1
  }

}
