package com.tanchuang.etl.odsTodwd

import com.alibaba.fastjson.JSON
import com.tanchuang.etl.bean.MemberRegType
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object InsertIntoMemReg {
  System.setProperty("HADOOP_USER_NAME", "atguigu")
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").enableHiveSupport().appName("etl").getOrCreate()
    import spark.implicits._
    val tableAddress = "hdfs://hadoop103:9000/data_warehouse/ods.db/"
    val tableName ="memberRegtype.log"
      val sc: SparkContext = spark.sparkContext
    val ds: Dataset[MemberRegType] = sc.textFile(tableAddress + tableName).filter(str => str !="").map(str => JSON.parseObject(str,classOf[MemberRegType])).toDS()
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    ds.write.mode(SaveMode.Append).format("snappy").insertInto("dwd.dwd_member_regtype")

  }

}
