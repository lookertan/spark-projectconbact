package com.tanchuang.etl.odsTodwd

import com.alibaba.fastjson.JSON
import com.tanchuang.etl.bean.Basewewebsite
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
 * 将 ods中的数据导入到dwd中
 * 1. 对用户名的手机号和密码脱敏处理
 * 2.所有的time分区字段的表，格式化当前日期为yyyyMMdd作为分区字段
 */
object InsertIntoWebSite {
  val tableAddress = "hdfs://hadoop103:9000/data_warehouse/ods.db/"

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    val tableName = "baswewebsite.log"
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .enableHiveSupport()
      .appName("etl")
      .getOrCreate()
    import spark.implicits._
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[String] = sc.textFile(tableAddress + tableName)

    val ds: Dataset[Basewewebsite] = rdd.map(str => JSON.parseObject(str, classOf[Basewewebsite])).toDS()
    ds.createTempView("temptable")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict") //非严格模式
    val frame: DataFrame = spark.sql(
      """
        |select siteid,sitename,siteurl,delete,unix_timestamp(createtime,"yyyy-MM-dd"),creator,dn
        | from temptable
        |""".stripMargin)

    frame.write.mode(SaveMode.Append).format("snappy").insertInto("dwd.dwd_base_website")


  }


}
