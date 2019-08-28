package com.Utils
import com.Tags.BusinessTag
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试类
  */
object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
//    val list= List("116.310003,39.991957")
//    val rdd = sc.makeRDD(list)
//    val bs = rdd.map(t=> {
//      val arr = t.split(",")
//      AMapUtil.getBusinessFromAmap(arr(0).toDouble,arr(1).toDouble)
//    })
//    bs.foreach(println)




    val ssc=new SQLContext(sc)
    val df=ssc.read.parquet("D:/MrOutput/Spark/basedata")
    df.rdd.map(row=>{
      val v1=row.getAs[String]("long")
      val v2=row.getAs[String]("lat")
      val business=BusinessTag.makeTags(row)
      business
//      (v1,v2)
    }).foreach(println)
  }
}
