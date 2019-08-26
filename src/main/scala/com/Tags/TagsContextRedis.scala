package com.Tags

import com.Utils.TagUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TagsContextRedis extends Serializable{
  def main(args: Array[String]): Unit = {

    //创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).getOrCreate()

    //广播停用词库
    val stopword = sc.textFile("D:\\千锋大数据培训\\10项目阶段\\项目day01\\Spark用户画像分析\\stopwords.txt").map((_,0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)

    //获取基础文件数据
    val df = spark.read.parquet("D:/MrOutput/Spark/basedata")
    //过滤符合Id的数据
    val titleData: RDD[(String, List[(String, Int)])] = df.filter(TagUtils.OneUserId).rdd
      // 接下来所有的标签都在内部实现
      .map(row => {
      // 取出用户Id
      val userId = TagUtils.getOneUserId(row)
      // 接下来通过row数据 打上 所有标签（按照需求）

      //广告标签
      val adList = TagsAd.makeTags(row)
      //appname标签
      val appList = TagsAppRedis.makeTags(row)
      //渠道标签
      val adplatList = TagsAdplat.makeTags(row)
      //设备标签
      val deviceList = TagsDevice.makeTags(row)
      //关键词标签
      val keywordsList = TagsKeywords.makeTags(row,bcstopword)
      //地域标签
      val locationList = TagsLocation.makeTags(row)

      (userId, adList:::appList:::adplatList:::deviceList:::keywordsList:::locationList)
    })

    titleData.reduceByKey((list1,list2)=>{
      (list1:::list2)
        //List(("APP爱奇艺",List(1,1)))
        .groupBy(_._1)
        .mapValues(_.size)
        .toList
    }).foreach(println)
//    titleData.foreach(println)

    sc.stop()
    spark.stop()
  }
}
