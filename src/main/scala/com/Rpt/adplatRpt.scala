package com.Rpt

import com.Utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object adplatRpt {
  def main(args: Array[String]): Unit = {
    // 创建一个集合保存输入和输出目录
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()


    val df: DataFrame = spark.read.parquet("D:/MrOutput/Spark/basedata")

    //渠道类型分析
    val adplatrest: RDD[(Int, List[Double])] = df.rdd.map(row => {
      // 把需要的字段全部取到
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // key 值  渠道ID
      val adplatformproviderid = row.getAs[Int]("adplatformproviderid")


      // 创建三个对应的方法处理九个指标

      (adplatformproviderid, RptUtils.request(requestmode, processnode) ++ RptUtils.click(requestmode, iseffective) ++ RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment))
    }).reduceByKey((x, y) => {
      List(x(0) + y(0), x(1) + y(1), x(2) + y(2), x(3) + y(3), x(4) + y(4), x(5) + y(5), x(6) + y(6), x(7) + y(7), x(8) + y(8))
    })
    adplatrest.foreach(println)
  }

}
