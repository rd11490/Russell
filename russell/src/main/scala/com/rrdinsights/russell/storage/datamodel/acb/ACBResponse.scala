package com.rrdinsights.russell.storage.datamodel.acb

import java.{lang => jl}

final case class ACBResponse(clock: String,
                             period: jl.Integer,
                             periodType: String,
                             inOT: jl.Integer,
                             tm: ACBTeams,
                             pbp: Seq[ACBPlayByPlayEvent],
                             leaddate: Seq[Seq[AnyVal]], //Seq[Seq[String], Seq[Int]]
                             disableMatch: jl.Integer,
                             attendance: jl.Integer,
                             periodsMax: jl.Integer,
                             periodLengthREGULAR: jl.Integer,
                             periodLengthOVERTIME: jl.Integer,
                             timeline: Seq[PLACEHOLDER],
                             scorers: PLACEHOLDER,
                             totalTimeAdded: jl.Integer,
                             totalIds: ACBIds,
                             officials_referee1: String,
                             officials_referee2: String,
                             officials_referee3: String)

final case class ACBTeams(team1: PLACEHOLDER, team2: PLACEHOLDER) //"1", "2"

final case class ACBIds(sBlocks: ACBIdsSection, sSteals: ACBIdsSection, sAssists: ACBIdsSection, sReboundsTotal: ACBIdsSection, sPoints: ACBIdsSection)

final case class ACBIdsSection(id1: ACBIdsPlayer, id2: ACBIdsPlayer, id3: ACBIdsPlayer, id4: ACBIdsPlayer, id5: ACBIdsPlayer) // "1","2","3","4","5"

final case class ACBIdsPlayer(
                               name: String,
                               tot: jl.Integer,
                               tno: jl.Integer,
                               pno: jl.Integer,
                               shirtNumber: String,
                               firstName: String,
                               familyName: String,
                               internationalFirstName: String,
                               internationalFamilyName: String,
                               firstNameInitial: String,
                               familyNameInitial: String,
                               internationalFirstNameInitial: String,
                               internationalFamilyNameInitial: String,
                               scoreboardName: String)


final case class ACBTeam(name: String,
                         nameInternational: String,
                         shortName: String,
                         shortNameInternational: String,
                         code: String,
                         codeInternational: String,
                         coach: String,
                         coachDetails: ACBCoachDetails,
                         assistcoach1: String,
                         assistcoach1Details: ACBCoachDetails,
                         assistcoach2: String,
                         assistcoach2Details: String,
                         asstSep: String,
                         score: jl.Integer,
                         full_score: jl.Integer,
                         tot_sMinutes: jl.Integer,
                         tot_sFieldGoalsMade: jl.Integer,
                         tot_sFieldGoalsAttempted: jl.Integer,
                         tot_sFieldGoalsPercentage: jl.Integer,
                         tot_sThreePointersMade: jl.Integer,
                         tot_sThreePointersAttempted: jl.Integer,
                         tot_sThreePointersPercentage: jl.Integer,
                         tot_sTwoPointersMade: jl.Integer,
                         tot_sTwoPointersAttempted: jl.Integer,
                         tot_sTwoPointersPercentage: jl.Integer,
                         tot_sFreeThrowsMade: jl.Integer,
                         tot_sFreeThrowsAttempted: jl.Integer,
                         tot_sFreeThrowsPercentage: jl.Integer,
                         tot_sReboundsDefensive: jl.Integer,
                         tot_sReboundsOffensive: jl.Integer,
                         tot_sReboundsTotal: jl.Integer,
                         tot_sAssists: jl.Integer,
                         tot_sTurnovers: jl.Integer,
                         tot_sSteals: jl.Integer,
                         tot_sBlocksReceived: jl.Integer,
                         tot_sFoulsPersonal: jl.Integer,
                         tot_sFoulsOn: jl.Integer,
                         tot_sPoints: jl.Integer,
                         tot_sPointsFromTurnovers: jl.Integer,
                         tot_sPointsSecondChance: jl.Integer,
                         tot_sPointsFastBreak: jl.Integer,
                         tot_sBenchPoints: jl.Integer,
                         tot_sPointsInThePaint: jl.Integer,
                         tot_sTimeLeading: jl.Integer,
                         tot_sBiggestLead: jl.Integer,
                         tot_sBiggestScoringRun: jl.Integer,
                         tot_sLeadChanges: jl.Integer,
                         tot_sTimesScoresLevel: jl.Integer,
                         tot_sFoulsTeam: jl.Integer,
                         tot_sReboundsTeam: jl.Integer,
                         tot_sReboundsTeamDefensive: jl.Integer,
                         tot_sReboundsTeamOffensive: jl.Integer,
                         tot_sTurnoversTeam: jl.Integer,
                         pl: Seq[PLACEHOLDER], //Players
                         tot_eff_1: jl.Integer,
                         tot_eff_2: jl.Integer,
                         tot_eff_3: jl.Integer,
                         tot_eff_4: jl.Integer,
                         tot_eff_5: jl.Integer,
                         tot_eff_6: jl.Integer,
                         tot_eff_7: jl.Integer,
                         p1_score: jl.Integer,
                         p2_score: jl.Integer,
                         p3_score: jl.Integer,
                         p4_score: jl.Integer,
                         fouls: jl.Integer,
                         timeouts: jl.Integer,
                         shot: Seq[ACBShotData],
                         scoring: Seq[PLACEHOLDER],
                         Ids: ACBIds)

final case class ACBShotData(
                              r: jl.Integer,
                              x: jl.Double,
                              y: jl.Double,
                              p: jl.Integer,
                              pno: jl.Integer,
                              tno: jl.Integer,
                              per: jl.Integer,
                              perType: String,
                              actionType: String,
                              subType: String,
                              player: String,
                              shirtNumber: String)

final case class ACBCoachDetails(firstName: String,
                                 familyName: String,
                                 internationalFirstName: String,
                                 internationalFamilyName: String,
                                 firstNameInitial: String,
                                 familyNameInitial: String,
                                 internationalFirstNameInitial: String,
                                 internationalFamilyNameInitial: String,
                                 scoreboardName: String)

final case class ACBPlayByPlayEvent(gt: String,
                                    s1: jl.Integer,
                                    s2: jl.Integer,
                                    lead: jl.Integer,
                                    tno: jl.Integer,
                                    period: jl.Integer,
                                    periodType: String,
                                    pno: jl.Integer,
                                    player: String,
                                    success: jl.Integer,
                                    actionType: String,
                                    qualifier: String,
                                    subType: String,
                                    scoring: jl.Integer,
                                    shirtNumber: String,
                                    firstName: String,
                                    familyName: String,
                                    internationalFirstName: String,
                                    internationalFamilyName: String,
                                    firstNameInitial: String,
                                    familyNameInitial: String,
                                    internationalFirstNameInitial: String,
                                    internationalFamilyNameInitial: String,
                                    scoreboardName: String)

final case class ACBScorers(team1: Seq[ACBScorer], team2: Seq[ACBScorer]) //"1", "2"

final case class ACBScorer(tno: jl.Integer,
                           pno: jl.Integer,
                           player: String,
                           shirtNumber: String,
                           firstName: String,
                           familyName: String,
                           internationalFirstName: String,
                           internationalFamilyName: String,
                           firstNameInitial: String,
                           familyNameInitial: String,
                           internationalFirstNameInitial: String,
                           internationalFamilyNameInitial: String,
                           scoreboardName: String,
                           times: Seq[ACBTimes],
                           summary: String)

final case class ACBTimes(gt: String, per: jl.Integer, perType: String)

final case class PLACEHOLDER()

/*
case class Person(serial: jl.Integer, firstName: String)
val rename = FieldSerializer[Person](renameTo("serial", "id"))
implicit val format: Formats = DefaultFormats + rename
write(Person(1, "Guest")) // actually returns {"id":1,"firstName":"Guest"}
 */