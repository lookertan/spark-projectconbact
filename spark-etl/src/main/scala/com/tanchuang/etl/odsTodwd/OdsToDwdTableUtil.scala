package com.tanchuang.etl.odsTodwd

import com.alibaba.fastjson.JSON
import com.tanchuang.etl.bean.{AdIdNameDn, Basewewebsite, Dwd_member, MemberRegType, PayMoney, VipLevel}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object OdsToDwdTableUtil {

  //从指定文件位置读取文件 写入到指定表中
  def readOdsLogInsertIntoDwdTable(spark:SparkSession,sourceAddress:String,destinationTable:String,className:String)={
    val lines: Dataset[String] = spark.read.textFile(sourceAddress)
    import spark.implicits._
    val value: Dataset[_ >: AdIdNameDn with Dwd_member with MemberRegType with PayMoney with VipLevel with Basewewebsite <: Product] = className match {
      case "AdIdNameDn" => lines.map(line => JSON.parseObject(line, classOf[AdIdNameDn]))
      case "Dwd_member" => lines.map(line => JSON.parseObject(line, classOf[Dwd_member]))
      case "MemberRegType" => lines.map(line => JSON.parseObject(line, classOf[MemberRegType]))
      case "PayMoney" => lines.map(line => JSON.parseObject(line, classOf[PayMoney]))
      case "VipLevel" => lines.map(line => JSON.parseObject(line, classOf[VipLevel]))
      case "Basewewebsite" => lines.map(line => JSON.parseObject(line, classOf[Basewewebsite]))
    }
    value.write.mode(SaveMode.Append).format("snappy").insertInto(destinationTable)
  }
}
