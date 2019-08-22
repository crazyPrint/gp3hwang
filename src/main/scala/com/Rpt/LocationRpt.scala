package com.Rpt

import com.Utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

/**
  * 地域分布指标
  */
object LocationRpt {
  def main(args: Array[String]): Unit = {

    // 判断路径是否正确
    if (args.length != 1) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //    val sQLContext = new SQLContext(sc)

    //    {
    //      //使用spark则无需设置snappy,因为默认
    //      val dff: DataFrame = spark.read.parquet("D:/MrOutput/Spark/basedata")
    //      dff.createOrReplaceTempView("log")
    //      spark.sql(
    //        """
    //          |select provincename,cityname,
    //          |    sum(case when REQUESTMODE=1 and PROCESSNODE>=1 then 1 else 0 end) OriginalRequestNum,
    //          |    sum(case when REQUESTMODE=1 and PROCESSNODE>=2 then 1 else 0 end) ValidRequestNum,
    //          |    sum(case when REQUESTMODE=1 and PROCESSNODE=3 then 1 else 0 end) SuccessRequestNum,
    //          |
    //          |    sum(case when ISEFFECTIVE=1 and ISBILLING=1 and ISBID=1 then 1 else 0 end) ParticipateBidNum,
    //          |    sum(case when ISEFFECTIVE=1 and ISBILLING=1 and ISWIN=1 and ADORDERID !=0 then 1 else 0 end) SuccessBidNum,
    //          |
    //          |    sum(case when REQUESTMODE=2 and ISEFFECTIVE=1 then 1 else 0 end) ShowNum,
    //          |    sum(case when REQUESTMODE=3 and ISEFFECTIVE=1 then 1 else 0 end) ClickNum,
    //          |
    //          |    sum(case when ISEFFECTIVE=1 and ISBILLING=1 and ISWIN=1 then WinPrice else 0 end)/1000 WinPriceNum,
    //          |    sum(case when ISEFFECTIVE=1 and ISBILLING=1 and ISWIN=1 then adpayment else 0 end)/1000 adpaymentNum
    //          |from log
    //          |group by provincename,cityname
    //        """.stripMargin)
    //        .write.mode(SaveMode.Overwrite).format("jdbc")
    //        .option("user", "root")
    //        .option("password", "123456")
    //        .option("dbtable", "sparkpro12")
    //        .option("url", "jdbc:mysql://localhost:3306/exam")
    //        .save()
    //    }


    // 获取数据
    val df: DataFrame = spark.read.parquet("D:/MrOutput/Spark/basedata")
    df.show()
    //将数据进行处理，统计各个指标
    val localtemp: RDD[((String, String), List[Double])] = df.rdd.map(row => {
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
      // key 值  是地域的省市
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")
      // 创建三个对应的方法处理九个指标

      ((pro, city), RptUtils.request(requestmode, processnode) ++ RptUtils.click(requestmode, iseffective) ++ RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment))
    })
    val localrest: RDD[((String, String), List[Double])] = localtemp.reduceByKey((x, y) => {
      List(x(0) + y(0), x(1) + y(1), x(2) + y(2), x(3) + y(3), x(4) + y(4), x(5) + y(5), x(6) + y(6), x(7) + y(7), x(8) + y(8))
    })
    localrest.foreach(println)



  }

}
