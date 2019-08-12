package com.tanchuang.second.pojo

import java.sql.Timestamp

import org.apache.spark.sql.types.Decimal


case class Qz_Chapter(chapterid: Int,
                      chapterlistid: Int,
                      chaptername: String,
                      sequence: String,
                      showstatus: String,
                      creator: String,
                      createtime: Timestamp,
                      courseid: Int,
                      chapternum: Int,
                      outchapterid: Int,
                      dt: String,
                      dn: String)

case class Qz_Chapter_List(
                            chapterlistid: Int,
                            chapterlistname: String,
                            courseid: Int,
                            chapterallnum: Int,
                            sequence: String,
                            status: String,
                            creator: String,
                            createtime: Timestamp,
                            dt: String,
                            dn: String
                          )


case class Qz_Point(
                     pointid: Int,
                     courseid: Int,
                     pointname: String,
                     pointyear: String,
                     chapter: String,
                     creator: String,
                     createtme: Timestamp,
                     status: String,
                     modifystatus: String,
                     excisenum: Int,
                     pointlistid: Int,
                     chapterid: Int,
                     sequence: String,
                     pointdescribe: String,
                     pointlevel: String,
                     typelist: String,
                     var score: String,
                     thought: String,
                     remid: String,
                     pointnamelist: String,
                     typelistids: String,
                     pointlist: String,
                     dt: String,
                     dn: String)


case class Qz_Point_Question(
                              pointid: Int,
                              questionid: Int,
                              questype: Int,
                              creator: String,
                              createtime: String,
                              dt: String,
                              dn: String
                            )

case class Qz_Site_Course(
                           sitecourseid: Int,
                           siteid: Int,
                           courseid: Int,
                           sitecoursename: String,
                           coursechapter: String,
                           sequence: String,
                           status: String,
                           creator: String,
                           createtime: Timestamp,
                           helppaperstatus: String,
                           servertype: String,
                           boardid: Int,
                           showstatus: String,
                           dt: String,
                           dn: String)

case class Qz_Course(
                      courseid: Int,
                      majorid: Int,
                      coursename: String,
                      coursechapter: String,
                      sequence: String,
                      isadvc: String,
                      creator: String,
                      createtime: Timestamp,
                      status: String,
                      chapterlistid: Int,
                      pointlistid: Int,
                      dt: String,
                      dn: String
                    )

case class Qz_Course_Edusubject(
                                 courseeduid: Int,
                                 edusubjectid: Int,
                                 courseid: Int,
                                 creator: String,
                                 createtime: Timestamp,
                                 majorid: Int,
                                 dt: String,
                                 dn: String
                               )

case class Qz_Website(
                       siteid: Int,
                       sitename: String,
                       domain: String,
                       sequence: String,
                       multicastserver: String,
                       templateserver: String,
                       status: String,
                       creator: String,
                       createtime: Timestamp,
                       multicastgateway: String,
                       multicastport: String,
                       dt: String,
                       dn: String
                     )

case class Qz_Major(
                     majorid: Int,
                     businessid: Int,
                     siteid: Int,
                     majorname: String,
                     shortname: String,
                     status: String,
                     sequence: String,
                     creator: String,
                     createtime: Timestamp,
                     column_sitetype: String,
                     dt: String,
                     dn: String
                   )

case class Qz_Business(
                        businessid: Int,
                        businessname: String,
                        sequence: String,
                        status: String,
                        creator: String,
                        createtime: Timestamp,
                        siteid: Int,
                        dt: String,
                        dn: String
                      )

case class Qz_Paper_View(
                          paperviewid: Int,
                          paperid: Int,
                          paperviewname: String,
                          paperparam: String,
                          openstatus: String,
                          explainurl: String,
                          iscontest: String,
                          contesttime: Timestamp,
                          conteststarttime: Timestamp,
                          contestendtime: Timestamp,
                          contesttimelimit: String,
                          dayiid: Int,
                          status: String,
                          creator: String,
                          createtime: Timestamp,
                          paperviewcatid: Int,
                          modifystatus: String,
                          description: String,
                          papertype: String,
                          downurl: String,
                          paperuse: String,
                          paperdifficult: String,
                          testreport: String,
                          paperuseshow: String,
                          dt: String,
                          dn: String
                        )

case class Qz_Center_Paper(
                            paperviewid: Int,
                            centerid: Int,
                            openstatus: String,
                            sequence: String,
                            creator: String,
                            createtime: Timestamp,
                            dt: String,
                            dn: String
                          )

case class Qz_Paper(
                     paperid: Int,
                     papercatid: Int,
                     courseid: Int,
                     paperyear: String,
                     chapter: String,
                     suitnum: String,
                     papername: String,
                     status: String,
                     creator: String,
                     createtime: Timestamp,
                     totalscore: String,
                     chapterid: Int,
                     chapterlistid: Int,
                     dt: String,
                     dn: String
                   )

case class Qz_Center(
                      centerid: Int,
                      centername: String,
                      centeryear: String,
                      centertype: String,
                      openstatus: String,
                      centerparam: String,
                      description: String,
                      creator: String,
                      createtime: Timestamp,
                      sequence: String,
                      provideuser: String,
                      centerviewtype: String,
                      stage: String,
                      dt: String,
                      dn: String
                    )

case class Qz_Question(
                        questionid: Int,
                        parentid: Int,
                        questypeid: Int,
                        quesviewtype: Int,
                        content: String,
                        answer: String,
                        analysis: String,
                        limitminute: String,
                        score: String,
                        splitscore: String,
                        status: String,
                        optnum: Int,
                        lecture: String,
                        creator: String,
                        createtime: String,
                        modifystatus: String,
                        attanswer: String,
                        questag: String,
                        vanalysisaddr: String,
                        difficulty: String,
                        quesskill: String,
                        vdeoaddr: String,
                        dt: String,
                        dn: String
                      )

case class Qz_Question_Type(
                             quesviewtype: Int,
                             viewtypename: String,
                             questypeid: Int,
                             description: String,
                             status: String,
                             creator: String,
                             createtime: Timestamp,
                             papertypename: String,
                             sequence: String,
                             remark: String,
                             splitscoretype: String,
                             dt: String,
                             dn: String
                           )

case class Qz_Member_Paper_Question(
                                     userid: Int,
                                     paperviewid: Int,
                                     chapterid: Int,
                                     sitecourseid: Int,
                                     questionid: Int,
                                     majorid: Int,
                                     useranswer: String,
                                     istrue: String,
                                     lasttime: Timestamp,
                                     opertype: String,
                                     paperid: Int,
                                     spendtime: Int,
                                     score: String,
                                     question_answer: Int,
                                     dt: String,
                                     dn: String
                                   )

case class Ads_Paper_Avgtimeandscore(
                                      paperviewid: Int,
                                      paperviewname: String,
                                      avgscore: String,
                                      avgspendtime: String,
                                      dt: String,
                                      dn: String
                                    )

case class Ads_Paper_Scoresegment_User(
                                        paperviewid: Int,
                                        paperviewname: String,
                                        var score: Decimal,
                                        userid: String,
                                        dt: String,
                                        dn: String
                                      )

case class Ads_Paper_Scoresegment_User_Result(
                                               paperviewid: Int,
                                               paperviewname: String,
                                               score_segment: String,
                                               userid: String,
                                               dt: String,
                                               dn: String
                                             )

case class Ads_User_Paper_Detail_Mid(
                                  paperviewid: Int,
                                  paperviewname: String,
                                  unpasscount: Int,
                                  passcount: Int,
                                  dt: String,
                                  dn: String
                                )

case class Ads_User_Paper_Detail(
                                  paperviewid: Int,
                                  paperviewname: String,
                                  unpasscount: Int,
                                  passcount: Int,
                                  rate: String,
                                  dt: String,
                                  dn: String
                                )

case class Ads_User_Question_Detail_first(
                                     questionid:Int,
                                     user_question_answer:Int,
                                     dt: String,
                                     dn: String
                                   )