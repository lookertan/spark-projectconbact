package com.tanchuang.etl.odsTodwd

import com.alibaba.fastjson.JSON
import com.tanchuang.etl.bean.AdIdNameDn
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object InsertIntoAdLog {
  val tableAddress = "hdfs://hadoop103:9000/data_warehouse/ods.db/"
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val tableName = "baseadlog.log"
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .enableHiveSupport()
      .appName("etl")
      .getOrCreate()
    import spark.implicits._
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
//    val sc: SparkContext = spark.sparkContext
    //减少对rdd的使用， dataFrame 和dataSet 默认使用了kryo 压缩 效率高
    //df ds rdd 中rdd的效率最低
    val startTime: Long = System.currentTimeMillis()
//    val rdd: RDD[String] = sc.textFile(tableAddress + tableName)

    //还可以配置成文件 从文件中读取到原始文件路径 和目标目标  className
    OdsToDwdTableUtil.readOdsLogInsertIntoDwdTable(spark,tableAddress+tableName,"dwd.dwd_base_ad","AdIdNameDn")
//    val ds: Dataset[AdIdNameDn] = spark.read.textFile(tableAddress + tableName)
      //      .map(line => JSON.parseObject(line,classOf[AdIdNameDn]) )
//      .mapPartitions(lines => lines.map(line => JSON.parseObject(line, classOf[AdIdNameDn])))
/*    val ds: Dataset[AdIdNameDn] = rdd.map(str => JSON.parseObject(str, classOf[AdIdNameDn]))
          .toDS()
    ds.show()*/
/*    val endTime: Long = System.currentTimeMillis()
    val costTime: Long = endTime-startTime*/

/*
    println("rdd 消耗时间: " + costTime) ==3805
    println("df 消耗时间: " + costTime)//未进行Rdd 类型转换  1348
    println("将Df内的元素进行转换" + costTime)  // mapPartition 1440  map 1412

   ds.write.mode(SaveMode.Append).format("snappy").insertInto("dwd.dwd_base_ad")
*/

  }
}
