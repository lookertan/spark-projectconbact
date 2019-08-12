package com.tanchuang.second.dao

import com.tanchuang.second.pojo.{Ads_Paper_Scoresegment_User, Ads_User_Question_Detail_first}
import org.apache.spark.sql.SparkSession

object ReadDwsTable {

  def readDwsTableChapterToPaperDetail(spark:SparkSession,dayTime:String)={
    spark.sql(
      s"""
         |select chaptername,chapternum,chapterallnum,outchapterid,chapterlistname,pointid,questype,pointyear,chapter,pointname,excisenum,pointdescribe,
         | pointlevel,typelist,point_score,thought,remid,pointnamelist,typelistids,pointlist,dn,chapterid
         | from dws.dws_qz_chapter
         | where dt ="$dayTime"
         |""".stripMargin)
  }

  def readDwsTableCourseToPaperDetail(spark:SparkSession,dayTime:String)={
    spark.sql(
      s"""
        |select  sitecourseid,courseid,
        | siteid,
        | sitecoursename,
        | coursechapter,
        | sequence as course_sequence,
        | status as course_stauts,
        | sitecourse_creator as course_creator,
        | sitecourse_createtime as course_createtime,
        | servertype,
        | helppaperstatus,
        | boardid,
        | showstatus,
        | majorid,
        | coursename,
        | isadvc,
        | chapterlistid,
        | pointlistid,
        | courseeduid,
        | edusubjectid,dn
        | from dws.dws_qz_course
        | where dt = "$dayTime"
        |""".stripMargin)
  }

  def readDwsTableMajorToPaperDetail(spark:SparkSession,dayTime:String)={
    spark.sql(
      s"""
        |
        |select majorid,businessid,
        | majorname,
        | shortname,
        | status as major_status,
        | sequence as major_sequence,
        | major_creator,
        | major_createtime,
        | businessname,
        | sitename,
        | domain,
        | multicastserver,
        | templateserver,
        | multicastgateway as multicastgatway,
        | multicastport,dn
        | from dws.dws_qz_major
        | where dt = "$dayTime"
        |""".stripMargin)
  }

  def readDwsTablePaperToPaperDetail(spark:SparkSession,dayTime:String)={
    spark.sql(
      s"""
        |select  paperviewid,
        | paperviewname,
        | paperparam,
        | openstatus,
        | explainurl,
        | iscontest,
        | contesttime,
        | conteststarttime,
        | contestendtime,
        | contesttimelimit,
        | dayiid,
        | status as paper_status,
        | paper_view_creator,
        | paper_view_createtime,
        | paperviewcatid,
        | modifystatus,
        | description,
        | paperuse,
        | testreport,
        | centerid,
        | sequence as paper_sequence,
        | centername,
        | centeryear,
        | centertype,
        | provideuser,
        | centerviewtype,
        | stage as paper_stage,
        | papercatid,
        | paperyear,
        | suitnum,
        | papername,
        | totalscore,dn
        | from dws.dws_qz_paper
        | where dt = "$dayTime"
        |""".stripMargin)
  }

  def readDwsTableQuestionToPaperDetail(spark:SparkSession,dayTime:String)={
    spark.sql(
      s"""
        |select questionid,parentid as question_parentid, questypeid,
        | quesviewtype,
        | content as question_content,
        | answer as question_answer,
        | analysis as question_analysis,
        | limitminute as question_limitminute,
        | score,
        | splitscore,
        | lecture,
        | creator as question_creator,
        | createtime as question_createtime,
        | modifystatus as question_modifystatus,
        | attanswer as question_attanswer,
        | questag as question_questag,
        | vanalysisaddr as question_vanalysisaddr,
        | difficulty as question_difficulty,
        | quesskill,
        | vdeoaddr,
        | description as question_description,
        | splitscoretype as  question_splitscoretype,dn
        |from dws.dws_qz_question
        |where dt = "$dayTime"
        |""".stripMargin)

  }


  def readDwsTablePaperDetailToPaperScore(spark:SparkSession,dayTime:String)={
    spark.sql(
      s"""
        |select paperviewid,paperviewname,score,spendtime,dt,dn
        | from dws.dws_user_paper_detail
        | where dt ="$dayTime"
        |""".stripMargin
    )
  }

  def readDwsTablePaperDetailToPaperMaxDetail(spark:SparkSession,dayTime:String)= {
    spark.sql(
      s"""
        |select paperviewid,paperviewname,score,dt,dn
        | from dws.dws_user_paper_detail
        | where dt = "$dayTime"
        |""".stripMargin)
  }

  def readDwsTablePaperDetailToTop3UserDetail(spark:SparkSession,dayTime:String)= {
    spark.sql(
      s"""
        |select userid,paperviewid,paperviewname,chaptername,pointname,sitecoursename,
        |coursename,majorname,shortname,papername,score,dt,dn
        | from dws.dws_user_paper_detail
        | where dt = "$dayTime"
        |""".stripMargin)
  }
  def readDwsTablePaperDetailToLow3UserDetail(spark:SparkSession,dayTime:String)= {

    spark.sql(
      s"""
         |select userid,paperviewid,paperviewname,chaptername,pointname,sitecoursename,
         |coursename,majorname,shortname,papername,score,dt,dn
         | from dws.dws_user_paper_detail
         | where dt = "$dayTime"
         |""".stripMargin)
  }

  def readDwsTablePaperDetailToPaperScoresegmentUser(spark:SparkSession,dayTime:String)= {
    import spark.implicits._
    spark.sql(
      s"""
        |select paperviewid,paperviewname,userid,score,dt,dn
        | from  dws.dws_user_paper_detail
        | where dt = "$dayTime"
        |""".stripMargin).as[Ads_Paper_Scoresegment_User]

  }

  def readDwsTablePaperDetailToUserPaperDetail(spark:SparkSession,dayTime:String)= {
    import spark.implicits._
    spark.sql(
      s"""
         |select paperviewid,paperviewname,userid,score,dt,dn
         | from  dws.dws_user_paper_detail
         | where dt = "$dayTime"
         |""".stripMargin).as[Ads_Paper_Scoresegment_User]

  }

  def readDwsTablePaperDetailToQuestionDetail(spark:SparkSession,dayTime:String)= {
    import spark.implicits._
    spark.sql(
      s"""
         |select questionid,user_question_answer,dt,dn
         | from  dws.dws_user_paper_detail
         | where dt = "$dayTime"
         |""".stripMargin).as[Ads_User_Question_Detail_first]

  }



}
