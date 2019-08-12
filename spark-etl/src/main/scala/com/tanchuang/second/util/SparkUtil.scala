package com.tanchuang.second.util

import com.alibaba.fastjson.JSON
import com.tanchuang.second.pojo.{Qz_Business, Qz_Center, Qz_Center_Paper, Qz_Chapter, Qz_Chapter_List, Qz_Course, Qz_Course_Edusubject, Qz_Major, Qz_Member_Paper_Question, Qz_Paper, Qz_Paper_View, Qz_Point, Qz_Point_Question, Qz_Question, Qz_Question_Type, Qz_Site_Course, Qz_Website}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object SparkUtil {
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
  val tableMemberPaperQuestion = "QzMemberPaperQuestion.log"

  def readOdsAllFiletoDwd(spark: SparkSession)={
    readOdsFileToDWd(spark, odsDbAddress + tableChapter, "dwd.dwd_qz_chapter", "Qz_Chapter")
    readOdsFileToDWd(spark, odsDbAddress + tableChapterList, "dwd.dwd_qz_chapter_list", "Qz_Chapter_List")
    readOdsFileToDWd(spark, odsDbAddress + tablePoint, "dwd.dwd_qz_point", "Qz_Point")
    readOdsFileToDWd(spark, odsDbAddress + tablePointQuestion, "dwd.dwd_qz_point_question", "Qz_Point_Question")
    readOdsFileToDWd(spark, odsDbAddress + tableSiteCourse, "dwd.dwd_qz_site_course", "Qz_Site_Course")
    readOdsFileToDWd(spark, odsDbAddress + tableCourse, "dwd.dwd_qz_course", "Qz_Course")
    readOdsFileToDWd(spark, odsDbAddress + tableCourseEdusubject, "dwd.dwd_qz_course_edusubject", "Qz_Course_Edusubject")
    readOdsFileToDWd(spark, odsDbAddress + tableWebsite, "dwd.dwd_qz_website", "Qz_Website")
    readOdsFileToDWd(spark, odsDbAddress + tableMajor, "dwd.dwd_qz_major", "Qz_Major")
    readOdsFileToDWd(spark, odsDbAddress + tableBusiness, "dwd.dwd_qz_business", "Qz_Business")
    readOdsFileToDWd(spark, odsDbAddress + tablePaperView, "dwd.dwd_qz_paper_view", "Qz_Paper_View")
    readOdsFileToDWd(spark, odsDbAddress + tableCenterPaper, "dwd.dwd_qz_center_paper", "Qz_Center_Paper")
    readOdsFileToDWd(spark, odsDbAddress + tablePaper, "dwd.dwd_qz_paper", "Qz_Paper")
    readOdsFileToDWd(spark, odsDbAddress + tableCenter, "dwd.dwd_qz_center", "Qz_Center")
    readOdsFileToDWd(spark, odsDbAddress + tableQuestion, "dwd.dwd_qz_question", "Qz_Question")
    readOdsFileToDWd(spark, odsDbAddress + tableQuestionType, "dwd.dwd_qz_question_type", "Qz_Question_Type")
    readOdsFileToDWd(spark, odsDbAddress + tableMemberPaperQuestion, "dwd.dwd_qz_member_paper_question", "Qz_Member_Paper_Question")
  }

  //统一使用这个 将ods层的数据导入到dwd层的数据中
  def readOdsFileToDWd(spark: SparkSession, odsFileAddress: String, desTableName: String, className: String) = {
    import spark.implicits._
    val ds: Dataset[String] = spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)

    (className match {
      case "Qz_Chapter" => ds.map(line => JSON.parseObject(line, classOf[Qz_Chapter]))
      case "Qz_Chapter_List" => ds.map(line => JSON.parseObject(line, classOf[Qz_Chapter_List]))
      case "Qz_Point" => ds.map(line => JSON.parseObject(line, classOf[Qz_Point])).map(point =>{
        Decimal(point.score.toDouble).toBigDecimal
        point.score = new Decimal().set(Decimal(point.score.toDouble).toBigDecimal,1,4).toString()
        point
      })
      case "Qz_Point_Question" => ds.map(line => JSON.parseObject(line, classOf[Qz_Point_Question]))
      case "Qz_Site_Course" => ds.map(line => JSON.parseObject(line, classOf[Qz_Site_Course]))
      case "Qz_Course" => ds.map(line => JSON.parseObject(line, classOf[Qz_Course]))
      case "Qz_Course_Edusubject" => ds.map(line => JSON.parseObject(line, classOf[Qz_Course_Edusubject]))
      case "Qz_Website" => ds.map(line => JSON.parseObject(line, classOf[Qz_Website]))
      case "Qz_Major" => ds.map(line => JSON.parseObject(line, classOf[Qz_Major]))
      case "Qz_Business" => ds.map(line => JSON.parseObject(line, classOf[Qz_Business]))
      case "Qz_Paper_View" => ds.map(line => JSON.parseObject(line, classOf[Qz_Paper_View]))
      case "Qz_Center_Paper" => ds.map(line => JSON.parseObject(line, classOf[Qz_Center_Paper]))
      case "Qz_Paper" => ds.map(line => JSON.parseObject(line, classOf[Qz_Paper]))
      case "Qz_Center" => ds.map(line => JSON.parseObject(line, classOf[Qz_Center]))
      case "Qz_Question" => ds.map(line => JSON.parseObject(line, classOf[Qz_Question]))
      case "Qz_Question_Type" => ds.map(line => JSON.parseObject(line, classOf[Qz_Question_Type]))
      case "Qz_Member_Paper_Question" => ds.map(line => JSON.parseObject(line, classOf[Qz_Member_Paper_Question]))
    }).coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)

  }

  //以下被弃用了
  @deprecated
  def readOdsFileToQzChapter(spark: SparkSession, odsFileAddress: String, desTableName: String) = {

    import spark.implicits._
    spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)
      .map(line => JSON.parseObject(line, classOf[Qz_Chapter]))
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)
  }

  @deprecated
  def readOdsFileToQzChapterList(spark: SparkSession, odsFileAddress: String, desTableName: String) = {

    import spark.implicits._
    spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)
      .map(line => JSON.parseObject(line, classOf[Qz_Chapter_List]))
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)
  }

  @deprecated
  def readOdsFileToQzPoint(spark: SparkSession, odsFileAddress: String, desTableName: String) = {
    import spark.implicits._
    spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)
      .map(line => JSON.parseObject(line, classOf[Qz_Point])).map(point => {
      point
    })
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)
  }

  @deprecated
  def readOdsFileToQzPointQuestion(spark: SparkSession, odsFileAddress: String, desTableName: String) = {
    import spark.implicits._
    spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)
      .map(line => JSON.parseObject(line, classOf[Qz_Point_Question]))
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)
  }

  @deprecated
  def readOdsFileToQzSiteCourse(spark: SparkSession, odsFileAddress: String, desTableName: String) = {
    import spark.implicits._
    spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)
      .map(line => JSON.parseObject(line, classOf[Qz_Site_Course]))
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)
  }

  @deprecated
  def readOdsFileToQzCourse(spark: SparkSession, odsFileAddress: String, desTableName: String) = {
    import spark.implicits._
    spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)
      .map(line => JSON.parseObject(line, classOf[Qz_Course]))
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)
  }

  @deprecated
  def readOdsFileToQzCourseEdusubject(spark: SparkSession, odsFileAddress: String, desTableName: String) = {
    import spark.implicits._
    spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)
      .map(line => JSON.parseObject(line, classOf[Qz_Course_Edusubject]))
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)
  }

  @deprecated
  def readOdsFileToQzWebsite(spark: SparkSession, odsFileAddress: String, desTableName: String) = {
    import spark.implicits._
    spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)
      .map(line => JSON.parseObject(line, classOf[Qz_Website]))
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)
  }

  @deprecated
  def readOdsFileToQzMajor(spark: SparkSession, odsFileAddress: String, desTableName: String) = {
    import spark.implicits._
    spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)
      .map(line => JSON.parseObject(line, classOf[Qz_Major]))
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)
  }

  @deprecated
  def readOdsFileToQzBusiness(spark: SparkSession, odsFileAddress: String, desTableName: String) = {
    import spark.implicits._
    spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)
      .map(line => JSON.parseObject(line, classOf[Qz_Business]))
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)
  }

  @deprecated
  def readOdsFileToQzPaperView(spark: SparkSession, odsFileAddress: String, desTableName: String) = {
    import spark.implicits._
    spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)
      .map(line => JSON.parseObject(line, classOf[Qz_Paper_View]))
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)
  }

  @deprecated
  def readOdsFileToQzCenterPaper(spark: SparkSession, odsFileAddress: String, desTableName: String) = {
    import spark.implicits._
    spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)
      .map(line => JSON.parseObject(line, classOf[Qz_Center_Paper]))
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)
  }

  @deprecated
  def readOdsFileToQzPaper(spark: SparkSession, odsFileAddress: String, desTableName: String) = {
    import spark.implicits._
    spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)
      .map(line => JSON.parseObject(line, classOf[Qz_Paper]))
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)
  }

  @deprecated
  def readOdsFileToQzCenter(spark: SparkSession, odsFileAddress: String, desTableName: String) = {
    import spark.implicits._
    spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)
      .map(line => JSON.parseObject(line, classOf[Qz_Center]))
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)
  }

  @deprecated
  def readOdsFileToQzQuestion(spark: SparkSession, odsFileAddress: String, desTableName: String) = {
    import spark.implicits._
    spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)
      .map(line => JSON.parseObject(line, classOf[Qz_Question]))
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)
  }

  @deprecated
  def readOdsFileToQzQuestionType(spark: SparkSession, odsFileAddress: String, desTableName: String) = {
    import spark.implicits._
    spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)
      .map(line => JSON.parseObject(line, classOf[Qz_Question_Type]))
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)
  }

  @deprecated
  def readOdsFileToQzMemberPaperQuestion(spark: SparkSession, odsFileAddress: String, desTableName: String) = {
    import spark.implicits._
    spark.read.textFile(odsFileAddress)
      .filter(_.nonEmpty)
      .map(line => JSON.parseObject(line, classOf[Qz_Member_Paper_Question]))
      .coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto(desTableName)
  }


}
