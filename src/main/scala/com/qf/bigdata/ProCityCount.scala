package com.qf.bigdata

import java.util

import com.qf.bigdata.util.{LoggerKIiller, SparkHelper}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object ProCityCount {
    def main(args: Array[String]): Unit = {
        LoggerKIiller.killer()
        //配置hadoop 路径
        System.setProperty("hadoop.home.dir","D:\\hadoop\\hadoop-2.7.6")
        //创建上下文
        val spark: SparkSession = SparkHelper.getSparkSession("dev")
        // 导入隐式转换
        import spark.implicits._
        //读取文件
        val df: DataFrame = spark.read.load("output/rs")
        import org.apache.spark.sql.functions._
        // 按照省市分组求Count
        val logDF: DataFrame = df.groupBy("provincename", "cityname").count()

        logDF.coalesce(1)
          .write
          .mode(SaveMode.Overwrite)
          .partitionBy("provincename","cityname")
          .json("city/rs")

    }
}
