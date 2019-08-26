package com.Tags

import com.Utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsApp extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    val brodic =args(1).asInstanceOf[Broadcast[Map[String, String]]]
    // 获取	App 名称
    var appnameKey=""
    val appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")

    if (!appname.equals("")) {
      appnameKey = appname
    } else {
      appnameKey = brodic.value.getOrElse(appid, "其他")
    }

    val adName = row.getAs[String]("adspacetypename")
    if (StringUtils.isNotBlank(adName)) {
      list :+= ("LN" + adName, 1)
    }
    list
  }
}