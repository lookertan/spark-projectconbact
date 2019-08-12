package com.tanchuang.second.app

import com.alibaba.fastjson.JSON
import com.tanchuang.second.pojo.{Qz_Chapter, Qz_Chapter_List, Qz_Course, Qz_Point, Qz_Point_Question, Qz_Site_Course, _}
import com.tanchuang.second.server.{AdsServer, DwdServer}
import com.tanchuang.second.util.SparkUtil
import jodd.typeconverter.Convert
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.codehaus.janino.Java

object SecondInsertIntoDwd {
  def main(args: Array[String]): Unit = {
    //设置 访问hadoop 的用户名
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val spark: SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("second insert into dwd")
      .getOrCreate()
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val dayTime = "20190722"
    //设置 非严格模式
/*    val sc: SparkContext = spark.sparkContext
    val odsDbAddress = "hdfs://hadoop103:9000/data_warehouse/ods.db/"
    val tableChapter = "QzChapter.log"
    val tableChapterList = "QzChapterList.log"
    val tablePoint = "QzPoint.log"
    val tablePointQuestion = "QzPointQuestion.log"
    val tableSiteCourse = "QzSiteCourse.log"
    val tableCourse = "QzCourse.log"
    val tableCourseEdusubject = "QzCourseEduSubject.log"
    val tableWebsite = "QzWebsite.log"
    val tableMajor = "QzMajor.log"
    val tableBusiness = "QzBusiness.log"
    val tablePaperView = "QzPaperView.log"
    val tableCenterPaper = "QzCenterPaper.log"
    val tablePaper = "QzPaper.log"
    val tableCenter = "QzCenter.log"
    val tableQuestion = "QzQuestion.log"
    val tableQuestionType = "QzQuestionType.log"
    val tableMemberPaperQuestion = "QzMemberPaperQuestion.log"*/
    /*
            readOdsFileToQzChapter(spark, odsDbAddress + tableChapter, "dwd.dwd_qz_chapter")
            readOdsFileToQzChapterList(spark,odsDbAddress+tableChapterList,"dwd.dwd_qz_chapter_list")
        readOdsFileToQzPoint(spark, odsDbAddress + tablePoint, "dwd.dwd_qz_point")
            readOdsFileToQzPointQuestion(spark,odsDbAddress+tablePointQuestion,"dwd.dwd_qz_point_question")
        readOdsFileToQzSiteCourse(spark, odsDbAddress + tableSiteCourse, "dwd.dwd_qz_site_course")
        readOdsFileToQzCourse(spark, odsDbAddress + tableCourse, "dwd.dwd_qz_course")

        readOdsFileToQzCourseEdusubject(spark, odsDbAddress + tableCourseEdusubject, "dwd.dwd_qz_course_edusubject")
        readOdsFileToQzWebsite(spark, odsDbAddress + tableWebsite, "dwd.dwd_qz_website")
        readOdsFileToQzMajor(spark, odsDbAddress + tableMajor, "dwd.dwd_qz_major")
        readOdsFileToQzBusiness(spark, odsDbAddress + tableBusiness, "dwd.dwd_qz_business")
        readOdsFileToQzPaperView(spark, odsDbAddress + tablePaperView, "dwd.dwd_qz_paper_view")
        readOdsFileToQzCenterPaper(spark, odsDbAddress + tableCenterPaper, "dwd.dwd_qz_center_paper")

        readOdsFileToQzPaper(spark, odsDbAddress + tablePaper, "dwd.dwd_qz_paper")
        readOdsFileToQzCenter(spark, odsDbAddress + tableCenter, "dwd.dwd_qz_center")
        readOdsFileToQzQuestion(spark, odsDbAddress + tableQuestion, "dwd.dwd_qz_question")

        readOdsFileToQzQuestionType(spark, odsDbAddress + tableQuestionType, "dwd.dwd_qz_question_type")
        readOdsFileToQzMemberPaperQuestion(spark, odsDbAddress + tableMemberPaperQuestion, "dwd.dwd_qz_member_paper_question")
    */
    //将ods的数据加入到dwd
//    SparkUtil.readOdsAllFiletoDwd(spark)

//    DwdServer.InsertIntoDwsChapter(spark, dayTime)
//    DwdServer.InsertIntoDwsQuestion(spark, dayTime)
//    DwdServer.InsertIntoDwsCourse(spark, dayTime)
//    DwdServer.InsertIntoDwsMajor(spark, dayTime)
//    DwdServer.InsertIntoDwsPaper(spark, dayTime)
//    DwdServer.InsertIntoDwsPaperDetail(spark, dayTime)

    //先初始化server
    AdsServer.init(spark,dayTime)
//    AdsServer.InsertIntoAdsPaperAvgTimeAndScore
//    AdsServer.InsertIntoAdsPaperMaxDetail
//    AdsServer.InsertIntoAdsTop3Userdetail
//    AdsServer.InsertIntoAdsLow3Userdetail
//    AdsServer.InsertIntoAdsPaperScoresegmentUser(spark)
//    AdsServer.InsertIntoAdsUserPaperDetail(spark)
    AdsServer.InsertIntoAdsUserQuestionDetail(spark)


  }


}
