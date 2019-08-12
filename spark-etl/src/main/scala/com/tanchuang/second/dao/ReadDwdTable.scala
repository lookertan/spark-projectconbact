package com.tanchuang.second.dao

import org.apache.spark.sql.SparkSession

object ReadDwdTable {
  def readDwdTable(spark: SparkSession, tableName: String, dayTime: String) = {
    spark.sql(
      s"""
         |select *
         | from $tableName
         | where dt= "$dayTime"
         |""".stripMargin)
  }

  def readDwdTableChapter(spark: SparkSession, dayTime: String) = {
    spark.sql(
      s"""
         |select chapterid, chapterlistid, chaptername, sequence,
         |      showstatus, creator as chapter_creator, createtime as chapter_createtime,
         |      courseid as chapter_courseid, chapternum,outchapterid,dt,dn
         | from dwd.dwd_qz_chapter
         | where dt= "$dayTime"
         |""".stripMargin)
  }

  def readDwdTableChapterList(spark: SparkSession, dayTime: String) = {
    spark.sql(
      s"""
         |select chapterlistid, chapterlistname, chapterallnum,status,dn
         | from dwd.dwd_qz_chapter_list
         | where dt= "$dayTime"
         |""".stripMargin)
  }

  def readDwdTablePoint(spark: SparkSession, dayTime: String) = {
    spark.sql(
      s"""
         |select dn,pointid,chapterid,
         | pointname,pointyear,chapter,excisenum,pointlistid,pointdescribe,pointlevel,typelist,score as point_score,thought,remid,
         | pointnamelist,typelistids,pointlist
         | from dwd.dwd_qz_point
         | where dt= "$dayTime"
         |""".stripMargin)
  }

  def readDwdTablePointQuestition(spark: SparkSession, dayTime: String) = {
    spark.sql(
      s"""
         |select dn,pointid,questionid,questype
         | from dwd.dwd_qz_point_question
         | where dt= "$dayTime"
         |""".stripMargin)
  }

  def readDwdTableQuestionToQuestion(spark: SparkSession, dayTime: String) = {
    spark.sql(
      s"""
         | select questionid ,parentid ,questypeid ,quesviewtype ,content
         | ,answer ,analysis ,limitminute ,score ,splitscore ,status
         | ,optnum ,lecture ,creator ,createtime ,modifystatus ,attanswer
         | ,questag,vanalysisaddr,difficulty ,quesskill,vdeoaddr,dt,dn
         | from dwd.dwd_qz_question
         | where dt= "$dayTime"
         |""".stripMargin)
  }

  def readDwdTableQuestionTypeToQuestion(spark: SparkSession, dayTime: String) = {
    spark.sql(
      s"""
         |select viewtypename,description,papertypename,splitscoretype,dn,questypeid
         | from dwd.dwd_qz_question_type
         | where dt= "$dayTime"
         |""".stripMargin)
  }

  def readDwdTableSiteCourse(spark: SparkSession, dayTime: String) = {
    spark.sql(
      s"""
         |select sitecourseid,siteid,courseid ,sitecoursename  ,coursechapter  ,sequence ,status ,
         |creator as sitecourse_creator ,createtime as sitecourse_createtime,helppaperstatus ,servertype ,boardid ,showstatus ,dt ,dn
         | from  dwd.dwd_qz_site_course
         |where dt = "$dayTime"
         |""".stripMargin)
  }

  def readDwdTableCourse(spark: SparkSession, dayTime: String) = {
      spark.sql(
        s"""
          |select courseid,dn,majorid,coursename,isadvc,chapterlistid,pointlistid
          | from dwd.dwd_qz_course
          | where dt ="$dayTime"
          |""".stripMargin)
  }

  def readDwdTableCourseEdusubject(spark: SparkSession, dayTime: String) = {
    spark.sql(
      s"""
        |select dn,courseid,edusubjectid,courseeduid
        | from dwd.dwd_qz_course_edusubject
        | where dt ="$dayTime"
        |""".stripMargin)
  }

  def readDwdTableMajor(spark: SparkSession, dayTime: String)={
    spark.sql(
      s"""
        |select majorid,businessid,siteid,majorname,shortname,status,sequence,creator as major_creator,createtime as major_createtime,dt,dn
        | from  dwd.dwd_qz_major
        | where dt ="$dayTime"
        |""".stripMargin)
  }

  def readDwdTableBussiness(spark: SparkSession, dayTime: String)={
    spark.sql(
      s"""
        |select businessname,siteid,dn,businessid
        | from dwd.dwd_qz_business
        | where dt = "$dayTime"
        |""".stripMargin)
  }

  def readDwdTableWebsiteToMajor(spark: SparkSession, dayTime: String)={
    spark.sql(
      """
        |select siteid,sitename,domain,multicastserver,templateserver,multicastgateway,multicastport,dn
        | from dwd.dwd_qz_website
        |""".stripMargin)
  }

  def readDwdTablePaperViewToPaper(spark:SparkSession,dayTime:String)={
    spark.sql(
      s"""
        |select paperviewid,paperid,paperviewname,paperparam,openstatus,explainurl,
        | iscontest,contesttime,conteststarttime,contestendtime,contesttimelimit,dayiid,status,
        | creator as paper_view_creator,createtime as paper_view_createtime,paperviewcatid,modifystatus,description,paperuse,
        | paperdifficult,testreport,paperuseshow,dt,dn
        | from  dwd.dwd_qz_paper_view
        | where dt ="$dayTime"
        |""".stripMargin)
  }

  def readDwdTableCenterPaperToPaper(spark:SparkSession,dayTime:String)={
    spark.sql(
      s"""
        |select paperviewid,centerid,dn
        | from dwd.dwd_qz_center_paper
        | where dt ="$dayTime"
        |""".stripMargin)
  }

  def readDwdTableCenterToPaper(spark:SparkSession,dayTime:String)={
    spark.sql(
      s"""
        |select centerid,sequence,centername,centeryear,centertype,provideuser,centerviewtype,stage,dn
        | from  dwd.dwd_qz_center
        | where dt = "$dayTime"
        |""".stripMargin)
  }

  def readDwdTablePaperToPaper(spark:SparkSession,dayTime:String)= {
  spark.sql(
    s"""
      |select papercatid,paperid,courseid,paperyear,suitnum,papername,totalscore,chapterid,chapterlistid,dn
      | from dwd.dwd_qz_paper
      | where dt ="$dayTime"
      |""".stripMargin)
  }

  def readDwdTablePaperQuestionToPaperDetail(spark:SparkSession,dayTime:String)={
    spark.sql(
      s"""
        |select *
        | from dwd.dwd_qz_member_paper_question
        | where dt = "$dayTime"
        |""".stripMargin)
  }



}
