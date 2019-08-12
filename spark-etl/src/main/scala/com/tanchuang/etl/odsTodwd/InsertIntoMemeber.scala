package com.tanchuang.etl.odsTodwd

import com.alibaba.fastjson.JSON
import com.tanchuang.etl.bean.{AdIdNameDn, Basewewebsite, Dwd_member, MemberRegType, PayMoney, VipLevel}
import com.tanchuang.etl.util.SparkUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object InsertIntoMemeber {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val tableAddress = "hdfs://hadoop103:9000/data_warehouse/ods.db/"
    val tableName = "member.log"
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .enableHiveSupport()
      .appName("etl")
      .getOrCreate()
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    SparkUtil.openCompression(spark)
    SparkUtil.useSnappyCompression(spark)
    val sc: SparkContext = spark.sparkContext

    readOdsLogInsertIntoDwdTable(spark,tableAddress + tableName,"dwd.Dwd_member","Dwd_member")

   /* import spark.implicits._
    val ds: Dataset[Dwd_member] = sc.textFile(tableAddress + tableName)
      .filter(str => str !="")
      .map(str =>JSON.parseObject(str,classOf[Dwd_member])).toDS()
    //进行数据的清洗
    val etlDs: Dataset[Dwd_member] = ds.map(Dwd_member => {
      Dwd_member.fullname = Dwd_member.fullname.charAt(0) + "**"
      Dwd_member.phone = Dwd_member.phone.substring(0, 3) + "*****"+Dwd_member.phone.substring(8)
      Dwd_member.password = "******"
      Dwd_member
    })
//    etlDs.collect().foreach(println)
    etlDs.write.mode(SaveMode.Append).format("snappy").insertInto("dwd.Dwd_member")
*/
  }
  
  

}
