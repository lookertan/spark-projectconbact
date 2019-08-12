package com.tanchuang.etl.dwsToads

import com.tanchuang.etl.bean.{Ads_Register_Adnamenum, Ads_Register_Appregurlnum, Ads_Register_Memberlevelnum, Ads_Register_Regsourcename, Ads_Register_Sitenamenum, Ads_Register_Top3MemberPay}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

//统计通过各注册跳转地址(appregurl)进行注册的用户数
object InsertIntoAppregurlnum {
  def main(args: Array[String]): Unit = {
    //设置访问用户名
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .enableHiveSupport()
      .appName("Insert Into Appregurlnum").getOrCreate()
    //设置非严格模式
    spark.sql("set  hive.exec.dynamic.partition.mode=nonstrict")

    val dayTime: String = args(0)

//    readMemeberIntoUrlNum(spark, "dws.dws_member", dayTime, "ads.ads_register_appregurlnum")
//    readMemberIntoNameNum(spark, "dws.dws_member", dayTime, "ads.ads_register_sitenamenum")
//    readMemberIntoRegsourceNameNum(spark, "dws.dws_member", dayTime, "ads.ads_register_regsourcenamenum")
//    readMemberIntoAdNameNum(spark, "dws.dws_member", dayTime, "ads.ads_register_adnamenum")
//    readMemberIntoLevelNum(spark, "dws.dws_member", dayTime, "ads.ads_register_memberlevelnum")
//    readMemberIntoTop3MemberPay(spark, "dws.dws_member", dayTime, "ads.ads_register_top3memberpay")
    readMemberIntoViplevelnum(spark, "dws.dws_member", dayTime, "ads.ads_register_viplevelnum")

  }


  //从指定表中读取指定日期的数据数据
  def readMemeberIntoUrlNum(spark: SparkSession, tableName: String, dayTime: String, destinationTableName: String) = {
    import spark.implicits._
    //当天的记录
    val df: DataFrame = spark.sql(
      s"""
         | select uid,dn,appregurl,dt
         | from $tableName
         | where dt = "$dayTime"
         |""".stripMargin)

    df.createTempView("t1")

    val result: Dataset[Ads_Register_Appregurlnum] = spark.sql(
      """
        | select appregurl,count(uid) num ,first(dt) dt,dn
        | from t1
        | group by appregurl,dn
        |""".stripMargin).as[Ads_Register_Appregurlnum]
    result.coalesce(3).write.mode(SaveMode.Append).insertInto(destinationTableName)

  }

  def readMemberIntoNameNum(spark: SparkSession, tableName: String, dayTime: String, destinationTableName: String) = {
    import spark.implicits._
    val df: DataFrame = spark.sql(
      s"""
         | select uid,dn,sitename,dt
         | from $tableName
         | where dt = "$dayTime"
         |""".stripMargin)
    df.createTempView("t2")
    val result: Dataset[Ads_Register_Sitenamenum] = spark.sql(
      """
        |  select sitename,count(uid) num,first(dt) dt,dn
        | from t2
        | group by sitename,dn
        |""".stripMargin).as[Ads_Register_Sitenamenum]

    result.coalesce(3).write.mode(SaveMode.Append).insertInto(destinationTableName)
  }

  def readMemberIntoRegsourceNameNum(spark: SparkSession, tableName: String, dayTime: String, destinationTableName: String) = {
    import spark.implicits._
    val df: DataFrame = spark.sql(
      s"""
         |select   regsource,uid,dt,dn
         |from $tableName
         |where dt = "$dayTime"
         |""".stripMargin)
    df.createTempView("t3")

    val result: Dataset[Ads_Register_Regsourcename] = spark.sql(
      """
        | select regsource as regsourcename,count(uid) num,first(dt) dt,dn
        | from t3
        | group by regsource,dn
        |""".stripMargin).as[Ads_Register_Regsourcename]
    result.coalesce(3).write.mode(SaveMode.Append).insertInto(destinationTableName)


  }

  def readMemberIntoAdNameNum(spark: SparkSession, tableName: String, dayTime: String, destinationTableName: String) = {
    import spark.implicits._
    val df: DataFrame = spark.sql(
      s"""
         |select adname,uid,dt,dn
         |from $tableName
         |where dt = "$dayTime"
         |""".stripMargin)
    df.createTempView("t3")

    val result: Dataset[Ads_Register_Adnamenum] = spark.sql(
      """
        | select adname ,count(uid) num,first(dt) dt,dn
        | from t3
        | group by adname,dn
        |""".stripMargin).as[Ads_Register_Adnamenum]
    result.coalesce(3).write.mode(SaveMode.Append).insertInto(destinationTableName)

  }

  def readMemberIntoLevelNum(spark: SparkSession, tableName: String, dayTime: String, destinationTableName: String) = {
    import spark.implicits._
    val df: DataFrame = spark.sql(
      s"""
         |select memberlevel,uid,dt,dn
         |from $tableName
         |where dt = "$dayTime"
         |""".stripMargin)
    df.createTempView("t3")

    val result: Dataset[Ads_Register_Memberlevelnum] = spark.sql(
      """
        | select memberlevel ,count(uid) num,first(dt) dt,dn
        | from t3
        | group by memberlevel,dn
        |""".stripMargin).as[Ads_Register_Memberlevelnum]
    result.coalesce(3).write.mode(SaveMode.Append).insertInto(destinationTableName)

  }

  def readMemberIntoTop3MemberPay(spark: SparkSession, tableName: String, dayTime: String, destinationTableName: String) = {
    import spark.implicits._
    val df: DataFrame = spark.sql(
      s"""
         |select uid,memberlevel,register,appregurl,regsource,adname,sitename,vip_level,paymoney,dt,dn,row_number() over(partition by dn,memberlevel sort by paymoney desc) number
         |from $tableName
         |where dt = "$dayTime"
         |""".stripMargin)
    df.createTempView("t4")

    val result: Dataset[Ads_Register_Top3MemberPay] = spark.sql(
      """
        | select uid,memberlevel,register,appregurl,regsource as regsourcename,adname,sitename,vip_level,cast(paymoney as decimal(10,4)) paymoney ,dt,dn,number as rownum
        | from t4
        | where number <=3
        |""".stripMargin).as[Ads_Register_Top3MemberPay]
    result.coalesce(3).write.mode(SaveMode.Append).insertInto(destinationTableName)

  }

  def readMemberIntoViplevelnum(spark: SparkSession, tableName: String, dayTime: String, destinationTableName: String) = {
    import org.apache.spark.sql.functions._
    spark.sql(
      s"""
        |select vip_level,dt,dn
        | from $tableName
        | where dt =$dayTime
        |""".stripMargin)
      .groupBy("vip_level","dn")
      .agg(first("dt").as("dt"),
        count("dt").as("num"))
      .selectExpr("vip_level","num","dt","dn")
      .coalesce(3).write.mode(SaveMode.Append).insertInto(destinationTableName)
  }

}
