package com.tanchuang.etl.dwdTodws

import com.tanchuang.etl.bean.Dws_Member
import com.tanchuang.etl.util.SparkUtil
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

//将dwd层的6张表插入到dws层的宽表中
//1.对dwd层的6张表进行合并，生成一张宽表，先使用Spark Sql实现。
// 2.有时间的同学需要使用DataFrame api实现功能,并对join进行优化。
object InsertIntoDwsMember {
  def main(args: Array[String]): Unit = {
      System.setProperty("HADOOP_USER_NAME","atguigu")
      val spark: SparkSession = SparkSession.builder()
        .master("local[*]")
        .enableHiveSupport()
        .appName("insert into dwd_member")
        .getOrCreate()


    insertIntoDwsMember(spark,args(0))


  }

  def insertIntoDwsMember(spark:SparkSession ,dayTime:String)={
    import spark.implicits._
    spark.sql(SparkUtil.OPEN_NONSTRICT)
    spark.sql("use dwd ")
    //1.获取member的全部数据
    //
    val dwd_member: DataFrame = spark.sql(
      s"""
        |select * from dwd_member where dt ="$dayTime"
        |""".stripMargin)
    dwd_member.createTempView("dwd_member")

    val dwd_member_regtype: DataFrame = spark.sql(
      s"""
        | select * from dwd_member_regtype where dt ="$dayTime"
        |""".stripMargin)
    dwd_member_regtype.createTempView("dwd_member_regtype")

    val dwd_base_ad: DataFrame = spark.sql(
      """
        |select * from dwd_base_ad
        |""".stripMargin)
    dwd_base_ad.createTempView("dwd_base_ad")

    val dwd_base_website: DataFrame = spark.sql(
      """
        |select * from dwd_base_website
        |""".stripMargin)
    dwd_base_website.createTempView("dwd_base_website")

    val dwd_pcentermempaymoney: DataFrame = spark.sql(
      s"""
        |select * from dwd_pcentermempaymoney where dt ="$dayTime"
        |""".stripMargin)
    dwd_pcentermempaymoney.createTempView("dwd_pcentermempaymoney")

    val dwd_vip_level: DataFrame = spark.sql(
      s"""
        |select * from dwd_vip_level
        |""".stripMargin)
    dwd_vip_level.createTempView("dwd_vip_level")

    val ds: Dataset[Dws_Member] = spark.sql(
      """
         | select t1.uid,last(t1.ad_id) ad_id,last(t1.fullname) fullname,last(t1.iconurl) iconurl,
         | last(t1.lastlogin) lastlogin,last(t1.mailaddr) mailaddr,last(t1.memberlevel) memberlevel,last(t1.password) password,
         | sum(cast(t1.paymoney as decimal(10,4))) paymoney,
         | last(t1.phone) phone,last(t1.qq) qq,last(t1.register) register,last(t1.regupdatetime) regupdatetime,last(t1.unitname) unitname,last(t1.userip) userip,last(t1.zipcode) zipcode,
         | last(t2.appkey) appkey,last(t2.appregurl) appregurl,last(t2.bdp_uuid) bdp_uuid,last(t2.createtime) reg_createtime,last(t2.domain) domain,last(t2.isranreg) isranreg,last(t2.regsource) regsource,
         | last(t3.adname) adname,
         | last(t5.siteid) siteid,last(t5.sitename) sitename,last(t5.siteurl) siteurl,last(t5.delete) site_delete,
         | last(t5.createtime) site_createtime,last(t5.creator) site_creator,
         | last(t6.vip_id) vip_id,last(t6.vip_level) vip_level,last(t6.start_time) vip_start_time,last(t6.end_time) vip_end_time,last(t6.last_modify_time) vip_last_modify_time,
         | last(t6.max_free) vip_max_free
         | ,last(t6.min_free) vip_min_free,last(t6.next_level) vip_next_level,last(t6.operator) vip_operator,
         | last(t1.dt) dt,t1.dn
         | from dwd_member t1 join dwd_member_regtype t2  join dwd_base_ad t3 join dwd_pcentermempaymoney t4 join dwd_base_website t5 join dwd_vip_level t6
         | on t1.uid=t2.uid and t1.dn = t2.dn
         |  and t1.ad_id= t3.adid and t1.dn = t3.dn
         |  and t1.uid= t4.uid and t1.dn = t4.dn
         |  and t4.siteid = t5.siteid and t4.dn = t5.dn
         |  and t4.vip_id = t6.vip_id and t4.dn = t6.dn
         |  group by t1.uid,t1.dn
          """.stripMargin).as[Dws_Member]
    ds.coalesce(3).write.mode(SaveMode.Append).format("snappy").insertInto("dws.dws_member")

  }



}
