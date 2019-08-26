package com.Tags

import com.Utils.Tag
import org.apache.spark.sql.Row

object TagsAdplat extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    //获取渠道ID
    val adplatformproviderid = row.getAs[Int]("adplatformproviderid")
    if (adplatformproviderid !=0){
      list :+= ("CN" + adplatformproviderid, 1)
    }
    list
  }
}