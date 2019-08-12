package com.tanchuang.etl.odsTodwd

import com.alibaba.fastjson.JSON
import com.tanchuang.etl.bean.PayMoney
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object InsertIntoPayMoney {
  System.setProperty("HADOOP_USER_NAME", "atguigu")
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").enableHiveSupport().appName("etl").getOrCreate()
    import spark.implicits._
    val tableAddress = "hdfs://hadoop103:9000/data_warehouse/ods.db/"
    val tableName = "pcentermempaymoney.log"
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[String] = sc.textFile(tableAddress + tableName)
    val ds: Dataset[PayMoney] = rdd.map(str => JSON.parseObject(str,classOf[PayMoney])).toDS()
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    ds.write.mode(SaveMode.Append).format("snappy").insertInto("dwd.dwd_pcentermempaymoney")

  }
}
