package com.tanchuang.etl.dwdTodws

import com.tanchuang.etl.bean.{MemberZipper, MemberZipperResult}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object InsertIntoMemberZip {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("insert into zip Member table")
      .enableHiveSupport().getOrCreate()
    insertIntoMemberZip(spark, args(0))

  }

  def insertIntoMemberZip(spark: SparkSession, dayTime: String) = {
    //将当天的数据
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("use dwd")
    import spark.implicits._
    // 每日增量订单数据 求每个用户的状态 最终状态
    val dayAddDS: Dataset[MemberZipper] = spark.sql(
      s"""
         | select t1.uid,sum(cast(t1.paymoney as decimal(10,4))) paymoney,max(t2.vip_level) as vip_level,
         | from_unixtime(unix_timestamp("$dayTime","yyyyMMdd"),"yyyy-MM-dd") as start_time,"9999-12-31" as end_time, t1.dn
         | from dwd_pcentermempaymoney t1 join dwd_vip_level t2
         | on t1.vip_id = t2.vip_id
         | and t1.dn = t2.dn
         | where dt = $dayTime
         | group by uid,t1.dn
         |""".stripMargin).as[MemberZipper]

    //得到全部的链表数据
    val memberZipperDs: Dataset[MemberZipper] = spark.sql(
      """
        |select * from dws.dws_member_zipper
        |""".stripMargin).as[MemberZipper]

    dayAddDS.union(memberZipperDs).groupByKey(member => member.uid + "_" + member.dn).mapGroups {
      case (key, ite) => {
        val membersList: List[MemberZipper] = ite.toList.sortBy(_.start_time.toString)
        val length: Int = membersList.size
        if (membersList.size > 1 && "9999-12-31".equals(membersList(length - 2))) {
          val oldLastRecord: MemberZipper = membersList(length - 2)
          val newLastRecord: MemberZipper = membersList(length - 1)
          oldLastRecord.end_time = newLastRecord.start_time
          newLastRecord.paymoney = oldLastRecord.paymoney + newLastRecord.paymoney
        }
        MemberZipperResult(membersList)
      }
    }.flatMap(_.membersList).coalesce(3).write.mode(SaveMode.Overwrite).format("snappy").insertInto("dws.dws_member_zipper")

  }
}
