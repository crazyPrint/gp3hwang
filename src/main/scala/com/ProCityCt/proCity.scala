package com.ProCityCt

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object proCity {
  def main(args: Array[String]): Unit = {
    // 判断路径是否正确
    if (args.length != 2) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath, outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    //使用spark则无需设置snappy,因为默认
    val spark = SparkSession.builder().config(conf).getOrCreate()


    val df: DataFrame = spark.read.parquet("D:/MrOutput/Spark/basedata")


    df.createOrReplaceTempView("log")
    val result1: DataFrame = spark.sql("select count(1) ct,provincename,cityname from log group by provincename,cityname order by ct desc")
    // 保存result1为json数据
    result1.coalesce(1).write.mode(SaveMode.Overwrite).partitionBy("provincename", "cityname") json ("D:/MrOutput/Spark/result1json")

    // 加载配置文件  需要使用对应的依赖
    val load = ConfigFactory.load()
    // 保存result1为mysql数据
    result1.write.mode(SaveMode.Overwrite).format("jdbc")
      .option("user", load.getString("jdbc.user"))
      .option("password", load.getString("jdbc.password"))
      .option("dbtable", "sparkpro1LOG")
      .option("url", load.getString("jdbc.url"))
      .save()

    result1.show()

    spark.stop()
  }
}
