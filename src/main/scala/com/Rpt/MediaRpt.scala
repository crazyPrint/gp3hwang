package com.Rpt


import java.sql.{Connection, DriverManager, PreparedStatement}

import com.Utils._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MediaRpt {
  def main(args: Array[String]): Unit = {
    // 创建一个集合保存输入和输出目录
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 获取数据
    val text: RDD[String] = sc.textFile("D:/MrInput/app_dict.txt")

    val texttemp: Map[String, String] = text.filter(_.length >= 4).map(line => {
      val row: Array[String] = line.split("\t")
      (row(4), row(1))
    }).collect().toMap


    //广播字典数据
    val brodic: Broadcast[Map[String, String]] = sc.broadcast(texttemp)

    val df: DataFrame = spark.read.parquet("D:/MrOutput/Spark/basedata")

    //媒体类型分析
    val resultRdd: RDD[(String, List[Double])] = df.rdd.map(row => {
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
      // key 值  应用id，应用名称
      val appid = row.getAs[String]("appid")
      val appname = row.getAs[String]("appname")

      var keyy = ""
      if (!appname.equals("")) {
        keyy = appname
      } else {
        keyy = brodic.value.getOrElse(appid, "其他")
      }

      // 创建三个对应的方法处理九个指标
      (keyy, RptUtils.request(requestmode, processnode) ++ RptUtils.click(requestmode, iseffective) ++ RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment))
    }).reduceByKey((x, y) => {
      List(x(0) + y(0), x(1) + y(1), x(2) + y(2), x(3) + y(3), x(4) + y(4), x(5) + y(5), x(6) + y(6), x(7) + y(7), x(8) + y(8))
    })


    resultRdd.foreachPartition(item => {
      //获取连接
      val conn: Connection = JDBCConnectePoolsTest.getConn()

      item.foreach(one => {
        val pstm: PreparedStatement = conn.prepareStatement("insert into  mediarpt values(?,?,?,?,?,?,?,?,?,?)")
        pstm.setString(1, one._1)
        pstm.setDouble(2, one._2(0))
        pstm.setDouble(3, one._2(1))
        pstm.setDouble(4, one._2(2))
        pstm.setDouble(5, one._2(3))
        pstm.setDouble(6, one._2(4))
        pstm.setDouble(7, one._2(5))
        pstm.setDouble(8, one._2(6))
        pstm.setDouble(9, one._2(7))
        pstm.setDouble(10, one._2(8))
        pstm.executeUpdate()
      })

      //还连接
      JDBCConnectePoolsTest.returnConn(conn)
    })
  }

}