package com.Tags

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

object DicToRedis {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    //数据获取RDD格式
    val text: RDD[String] = sc.textFile("D:/MrInput/app_dict.txt")
    val data2Redis: RDD[(String, String)] = text.filter(_.length >= 5).map(line => {
      val row: Array[String] = line.split("\t")
      (row(4), row(1))
    })

    val redisCoon = (it : Iterator[(String,String)]) => {
      val jedis = new Jedis("hadoop01", 6379)
      it.foreach(tup => {
        jedis.hset("dict_appname", tup._1, tup._2)
      })
      jedis.close()
    }

    data2Redis.foreachPartition(redisCoon)
    sc.stop()
  }
}