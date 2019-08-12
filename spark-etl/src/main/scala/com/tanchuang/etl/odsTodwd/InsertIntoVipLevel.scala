package com.tanchuang.etl.odsTodwd

import com.alibaba.fastjson.JSON
import com.tanchuang.etl.bean.VipLevel
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object InsertIntoVipLevel {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val spark: SparkSession = SparkSession.builder().master("local[*]").enableHiveSupport().appName("etl").getOrCreate()
    import spark.implicits._
    val tableAddress = "hdfs://hadoop103:9000/data_warehouse/ods.db/"
    val tableName ="pcenterMemViplevel.log"
    val sc: SparkContext = spark.sparkContext
    val ds: Dataset[VipLevel] = sc.textFile(tableAddress + tableName).map(str => JSON.parseObject(str,classOf[VipLevel])).toDS()

    ds.createTempView("temptable")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict ")
    val df: DataFrame = spark.sql(
      """
        |select vip_id,vip_level,unix_timestamp(start_time,"yyyy-MM-dd"),unix_timestamp(end_time,"yyyy-MM-dd"),
        | unix_timestamp(last_modify_time,"yyyy-MM-dd"),max_free,min_free,next_level,operator,dn
        |  from temptable
        |""".stripMargin)
    df.write.mode(SaveMode.Append).format("snappy").insertInto("dwd.dwd_vip_level")

  }
}
