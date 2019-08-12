package com.tanchuang.etl.util

import org.apache.spark.sql.SparkSession

object SparkUtil {
    val OPEN_NONSTRICT ="set hive.exec.dynamic.partition.mode=nonstrict"
    def setMaxPartitionx(spark:SparkSession)={
        spark.sql("set hive.exec.dynamic.partition=true")
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        spark.sql("set hive.exec.max.dynamic.partitions=100000")
        spark.sql("set hive.exec.max.dynamic.partitions.pernode=100000")
        spark.sql("set hive.exec.max.created.files=100000")
    }

    def openCompression(spark:SparkSession)={
        spark.sql("set mapred.output.compress=true")
        spark.sql("set hive.exec.compress.output=true")
    }

    def openDynamicPartition(spark:SparkSession)={
        spark.sql("set hive.exec.dynamic.partition=true")
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    }

    def useLzoCompression(spark:SparkSession)={
        spark.sql("set io.compression.codec.lzo.class=com.hadoop.compression.lzo.LzoCodec")
        spark.sql("set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec")
    }

    def useSnappyCompression(spark:SparkSession)={
        spark.sql("set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec");
        spark.sql("set mapreduce.output.fileoutputformat.compress=true")
        spark.sql("set mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec")
    }


}
