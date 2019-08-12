package com.tanchuang.second.server

import com.tanchuang.second.dao.ReadDwsTable
import com.tanchuang.second.pojo.{Ads_Paper_Scoresegment_User, Ads_Paper_Scoresegment_User_Result, Ads_User_Paper_Detail_Mid, Ads_User_Question_Detail_first}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark
import org.apache.spark.sql.types.Decimal

object AdsServer {
  var spark: SparkSession = _
  var dayTime: String = _

  import org.apache.spark.sql.functions._

  def init(sp: SparkSession, dt: String) = {
    spark = sp
    dayTime = dt
  }

  def InsertIntoAdsPaperAvgTimeAndScore() = {
    //    paperviewid,paperviewname,score,spendtime,dt,dn
    ReadDwsTable.readDwsTablePaperDetailToPaperScore(spark, dayTime)
      .groupBy("paperviewid", "dn")
      .agg(first("paperviewname").as("paperviewname"),
        avg("score").as("avgscore"),
        avg("spendtime").as("avgspendtime"),
        first("dt").as("dt")
      ).selectExpr("paperviewid", "paperviewname", "avgscore", "avgspendtime", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_paper_avgtimeandscore")
    //表中数据太少 减少分区 防止在hdfs上产生小文件
  }

  def InsertIntoAdsPaperMaxDetail() = {
    //    paperviewid,paperviewname,score,dt,dn
    ReadDwsTable.readDwsTablePaperDetailToPaperMaxDetail(spark, dayTime)
      .groupBy("paperviewid", "dn")
      .agg(first("paperviewname").as("paperviewname"),
        max("score").as("maxscore"),
        min("score").as("minscore"),
        first("dt").as("dt"))
      .selectExpr("paperviewid", "paperviewname", "maxscore", "minscore", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_paper_maxdetail")

  }

  def InsertIntoAdsTop3Userdetail = {

    import org.apache.spark.sql.functions._
    ReadDwsTable.readDwsTablePaperDetailToTop3UserDetail(spark, dayTime)
      .withColumn("rk",
        rank() over (Window.partitionBy("paperviewid", "dn") orderBy (desc("score"))))
      .where("rk < 4")
      .selectExpr("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename",
        "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Append).insertInto("ads.ads_top3_userdetail")
  }

  def InsertIntoAdsLow3Userdetail = {
    ReadDwsTable.readDwsTablePaperDetailToLow3UserDetail(spark, dayTime)
      .withColumn("rk",
        rank() over (Window.partitionBy("paperviewid", "dn") orderBy (asc("score"))))
      .where("rk < 4")
      .selectExpr("userid", "paperviewid", "paperviewname", "chaptername", "pointname", "sitecoursename",
        "coursename", "majorname", "shortname", "papername", "score", "rk", "dt", "dn")
      .coalesce(1).write.mode(SaveMode.Overwrite).insertInto("ads.ads_low3_userdetail")
  }

  def InsertIntoAdsPaperScoresegmentUser(spark1: SparkSession) = {
    import spark1.implicits._
    ReadDwsTable.readDwsTablePaperDetailToPaperScoresegmentUser(spark1, dayTime)
      .map(user => {
        var score_segment: String = ""
        val score: Double = user.score.toDouble
        if (0 <= score && score < 20) {
          score_segment = "0-20"
        } else if (20 <= score && score < 40) {
          score_segment = "20-40"
        } else if (40 <= score && score < 60) {
          score_segment = "40-60"
        } else if (60 <= score && score < 80) {
          score_segment = "60-80"
        } else {
          score_segment = "80-100"
        }
        Ads_Paper_Scoresegment_User_Result(user.paperviewid, user.paperviewname, score_segment, user.userid, user.dt, user.dn)
      })
      .groupBy("paperviewid", "score_segment", "dn")
      .agg(first("paperviewname").as("paperviewname"),
        first("dt").as("dt"),
        concat_ws(",", collect_list("userid")).as("userids")
      )
      .selectExpr("paperviewid", "paperviewname", "score_segment", "userids", "dt", "dn")
      .coalesce(3).write.mode(SaveMode.Append).insertInto("ads.ads_paper_scoresegment_user")
  }

  def InsertIntoAdsUserPaperDetail(spark1: SparkSession) = {
    import spark1.implicits._
    ReadDwsTable.readDwsTablePaperDetailToUserPaperDetail(spark1, dayTime)
      .map(user => {
        var unpasscount: Int = 0
        var passcount: Int = 0
        if (user.score.toDouble > 60) {
          unpasscount = 1
        } else {
          passcount = 1
        }
        Ads_User_Paper_Detail_Mid(user.paperviewid, user.paperviewname, unpasscount, passcount, user.dt, user.dn)
      }).groupBy("paperviewid", "dn")
      .agg(first("paperviewname").as("paperviewname"),
        sum("unpasscount").as("unpasscount"),
        sum("passcount").as("passcount"),
        first("dt").as("dt")
      )
      .selectExpr("paperviewid", "paperviewname", "unpasscount", "passcount", "cast(unpasscount/passcount as decimal(4,2)) as rate", "dt", "dn")
      .coalesce(3).write.mode(SaveMode.Append).insertInto("ads.ads_user_paper_detail")

  }

  def InsertIntoAdsUserQuestionDetail(spark1: SparkSession) = {
    import spark1.implicits._

    ReadDwsTable.readDwsTablePaperDetailToQuestionDetail(spark1, dayTime)
      .groupByKey(detail => detail.questionid + "_" + detail.dn)
      .mapGroups{
        case (key,iter)=> {
          val keys: Array[String] = key.split("_")
          val questionid = keys(0).toInt
          val dn: String = keys(1)
          val detail_firsts: List[Ads_User_Question_Detail_first] = iter.toList
          val dt: String = detail_firsts.map(first => first.dt).head
          val rightcount: Int = detail_firsts.map(first => first.user_question_answer).sum
          val errcount: Int = detail_firsts.size -rightcount
          (questionid,errcount,rightcount,dt,dn)
        }
      }.selectExpr("_1 as questionid","_2","_3","cast(_2/(_2 + _3) as decimal(4,2)) as rate","_4","_5")
      .coalesce(3).write.mode(SaveMode.Overwrite).insertInto("ads.ads_user_question_detail")

  }


  def InsertIntoAds_Register_Viplevelnum(spark1: SparkSession)={

  }

}
