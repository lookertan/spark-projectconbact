package com.tanchuang.second.server

import com.tanchuang.second.dao.{ReadDwdTable, ReadDwsTable}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

object DwdServer {
  def InsertIntoDwsChapter(spark: SparkSession, dayTime: String) = {
    /*    val chapterDf: DataFrame = ReadDwdTable.readDwdTable(spark, "dwd.dwd_qz_chapter", dayTime).select(
          "chapterid", "chapterlistid", "chaptername", "sequence",
          "showstatus", "creator", "createtime ",
          "courseid", "chapternum", "chapterallnum","outchapterid","dt","dn"
        )

        val chapterListDF: DataFrame = ReadDwdTable.readDwdTable(spark, "dwd.dwd_qz_chapter_list", dayTime).select(
          "chapterlistid", "chapterlistname", "chapterallnum","status","dn"
        )

        val pointDF: DataFrame = ReadDwdTable.readDwdTable(spark, "dwd.dwd_qz_point", dayTime).select(
          "dn","pointid","chapterid",
          "pointname","pointyear","chapter","excisenum","pointlistid","pointdescribe","pointlevel","typelist","score","thought","remidremid",
          "pointnamelist","typelistids","pointlist"
        )

        val pointQuestionDF: DataFrame = ReadDwdTable.readDwdTable(spark, "dwd.dwd_qz_point_question", dayTime).select(
          "dn","pointid","questionid","questype"
        )*/
    val chapterDf: DataFrame = ReadDwdTable.readDwdTableChapter(spark, dayTime)
    val chapterListDF: DataFrame = ReadDwdTable.readDwdTableChapterList(spark, dayTime)
    val pointDF: DataFrame = ReadDwdTable.readDwdTablePoint(spark, dayTime)
    val pointQuestionDF: DataFrame = ReadDwdTable.readDwdTablePointQuestition(spark, dayTime)

    chapterDf.join(chapterListDF, Seq("chapterlistid", "dn")).join(pointDF, Seq("dn", "chapterid")).join(pointQuestionDF, Seq("pointid", "dn")).select(
      "chapterid", "chapterlistid", "chaptername", "sequence", "showstatus", "status", "chapter_creator", "chapter_createtime",
      "chapter_courseid", "chapternum", "chapterallnum", "outchapterid", "chapterlistname", "pointid", "questionid", "questype",
      "pointname", "pointyear", "chapter", "excisenum", "pointlistid", "pointdescribe", "pointlevel", "typelist", "point_score", "thought", "remid", "pointnamelist", "typelistids", "pointlist",
      "dt", "dn"
    ).coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto("dws.dws_qz_chapter")
  }


  def InsertIntoDwsQuestion(spark: SparkSession, dayTime: String) = {
    val questitionDF: DataFrame = ReadDwdTable.readDwdTableQuestionToQuestion(spark, dayTime)
    val questionTypeDF: DataFrame = ReadDwdTable.readDwdTableQuestionTypeToQuestion(spark, dayTime)

    questitionDF.join(questionTypeDF, Seq("questypeid", "dn")).select(
      "questionid", "parentid", "questypeid",
      "quesviewtype", "content", "answer", "analysis", "limitminute", "score", "splitscore",
      "status", "optnum", "lecture", "creator", "createtime", "modifystatus", "attanswer", "questag", "vanalysisaddr",
      "difficulty", "quesskill", "vdeoaddr", "viewtypename", "description", "papertypename", "splitscoretype", "dt", "dn"
    ).write.mode(SaveMode.Append).format("snappy").insertInto("dws.dws_qz_question")

  }

  def InsertIntoDwsCourse(spark: SparkSession, dayTime: String) = {
    val siteCourse: DataFrame = ReadDwdTable.readDwdTableSiteCourse(spark, dayTime)
    val courseDF: DataFrame = ReadDwdTable.readDwdTableCourse(spark, dayTime)
    val courseEduDF: DataFrame = ReadDwdTable.readDwdTableCourseEdusubject(spark, dayTime)

    siteCourse.join(courseDF, Seq("courseid", "dn")).join(courseEduDF, Seq("courseid", "dn")).select(
      "sitecourseid", "siteid", "courseid", "sitecoursename", "coursechapter", "sequence", "status",
      "sitecourse_creator", "sitecourse_createtime", "helppaperstatus", "servertype", "boardid", "showstatus",
      "majorid", "coursename", "isadvc", "chapterlistid", "pointlistid",
      "courseeduid", "edusubjectid", "dt", "dn"
    )
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto("dws.dws_qz_course")

  }

  def InsertIntoDwsMajor(spark: SparkSession, dayTime: String) = {
    val majorDF: DataFrame = ReadDwdTable.readDwdTableMajor(spark, dayTime).as("major")
    val bussinessDF: DataFrame = ReadDwdTable.readDwdTableBussiness(spark, dayTime).as("business")
    val websiteDF: DataFrame = ReadDwdTable.readDwdTableWebsiteToMajor(spark, dayTime)

    majorDF.join(bussinessDF, Seq("businessid", "dn")).join(websiteDF, Seq("siteid", "dn")).selectExpr(
      "majorid", "businessid", "major.siteid", "majorname", "shortname", "status",
      "sequence", "major_creator", "major_createtime", "businessname",
      "sitename", "domain", "multicastserver", "templateserver", "multicastgateway", "multicastport"
      , "dt", "dn"
    ).coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto("dws.dws_qz_major")

  }

  def InsertIntoDwsPaper(spark: SparkSession, dayTime: String) = {
    val paperViewDF: DataFrame = ReadDwdTable.readDwdTablePaperViewToPaper(spark, dayTime)
    val centerPaperDF: DataFrame = ReadDwdTable.readDwdTableCenterPaperToPaper(spark, dayTime)
    val centerDF: DataFrame = ReadDwdTable.readDwdTableCenterToPaper(spark, dayTime)
    val paperDF: DataFrame = ReadDwdTable.readDwdTablePaperToPaper(spark, dayTime)

    paperViewDF.join(centerPaperDF, Seq("paperviewid", "dn")).join(centerDF, Seq("centerid", "dn")).join(paperDF, Seq("paperid", "dn"))
      .select(
        "paperviewid", "paperid", "paperviewname", "paperparam", "openstatus", "explainurl", "iscontest", "contesttime",
        "conteststarttime", "contestendtime", "contesttimelimit", "dayiid", "status", "paper_view_creator", "paper_view_createtime",
        "paperviewcatid", "modifystatus", "description", "paperuse", "paperdifficult", "testreport", "paperuseshow",
        "centerid", "sequence", "centername", "centeryear", "centertype", "provideuser", "centerviewtype", "stage",
        "papercatid", "courseid", "paperyear", "suitnum", "papername", "totalscore", "chapterid", "chapterlistid",
        "dt", "dn"
      ).coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto("dws.dws_qz_paper")

  }

  def InsertIntoDwsPaperDetail(spark: SparkSession, dayTime: String) = {
    val paperQuestionDf: Dataset[Row] = ReadDwdTable.readDwdTablePaperQuestionToPaperDetail(spark, dayTime)
      .selectExpr("userid","question_answer as user_question_answer","lasttime","spendtime","useranswer","questionid","istrue","dn","opertype",
        "chapterid","sitecourseid","majorid","paperviewid","paperid","dt")
      .repartition(20).as("pq")
    val chapterDf: DataFrame = ReadDwsTable.readDwsTableChapterToPaperDetail(spark, dayTime).as("chapter").repartition(20)
    val courseDf: Dataset[Row] = ReadDwsTable.readDwsTableCourseToPaperDetail(spark,dayTime).as("course").repartition(20)
    val majorDf: DataFrame = ReadDwsTable.readDwsTableMajorToPaperDetail(spark,dayTime).as("major").repartition(20)
    val paperDf: Dataset[Row] = ReadDwsTable.readDwsTablePaperToPaperDetail(spark,dayTime).as("paper").repartition(20)
    val questionDf: Dataset[Row] = ReadDwsTable.readDwsTableQuestionToPaperDetail(spark,dayTime).as("question").repartition(20)
    import org.apache.spark.sql.functions._
    paperQuestionDf
      .join(chapterDf,Seq("chapterid","dn"))
      .join(courseDf,Seq("sitecourseid","dn"))
      .join(majorDf,Seq("majorid","dn"))
      .join(paperDf,Seq("paperviewid","dn"))
      .join(questionDf,Seq("questionid","dn"))
      .selectExpr(
        "userid",
        "courseid",
        "questionid",
        "useranswer",
        "istrue",
        "lasttime",
        "opertype",
        "paperid",
        "spendtime",
        "chapterid",
        "chaptername",
        "chapternum",
        "chapterallnum",
        "outchapterid",
        "chapterlistname",
        "pointid",
        "questype",
        "pointyear",
        "chapter",
        "pointname",
        "excisenum",
        "pointdescribe",
        "pointlevel",
        "typelist",
        "point_score",
        "thought",
        "remid",
        "pointnamelist",
        "typelistids",
        "pointlist",
        "sitecourseid",
        "siteid",
        "sitecoursename",
        "coursechapter",
        "course_sequence",
        "course_stauts",
        "course_creator",
        "course_createtime",
        "servertype",
        "helppaperstatus",
        "boardid",
        "showstatus",
        "pq.majorid",
        "coursename",
        "isadvc",
        "chapterlistid",
        "pointlistid",
        "courseeduid",
        "edusubjectid",
        "businessid",
        "majorname",
        "shortname",
        "major_status",
        "major_sequence",
        "major_creator",
        "major_createtime",
        "businessname",
        "sitename",
        "domain",
        "multicastserver",
        "templateserver",
        "multicastgatway",
        "multicastport",
        "paperviewid",
        "paperviewname",
        "paperparam",
        "openstatus",
        "explainurl",
        "iscontest",
        "contesttime",
        "conteststarttime",
        "contestendtime",
        "contesttimelimit",
        "dayiid",
        "paper_status",
        "paper_view_creator",
        "paper_view_createtime",
        "paperviewcatid",
        "modifystatus",
        "description",
        "paperuse",
        "testreport",
        "centerid",
        "paper_sequence",
        "centername",
        "centeryear",
        "centertype",
        "provideuser",
        "centerviewtype",
        "paper_stage",
        "papercatid",
        "paperyear",
        "suitnum",
        "papername",
        "totalscore",
        "question_parentid",
        "questypeid",
        "quesviewtype",
        "question_content",
        "question_answer",
        "question_analysis",
        "question_limitminute",
        "score",
        "splitscore",
        "lecture",
        "question_creator",
        "question_createtime",
        "question_modifystatus",
        "question_attanswer",
        "question_questag",
        "question_vanalysisaddr",
        "question_difficulty",
        "quesskill",
        "vdeoaddr",
        "question_description",
        "question_splitscoretype",
        "user_question_answer",
        "dt",
        "dn"
      )
      .coalesce(3)
      .write.mode(SaveMode.Append)
      .format("snappy")
      .insertInto("dws.dws_user_paper_detail")

  }
}
