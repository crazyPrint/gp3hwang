package com.Tags

import com.Utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsDevice extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    //获取操作系统
    val clienttype = row.getAs[Int]("client")

    if (clienttype == 1) list :+= ("D00010001", 1)
    else if (clienttype == 2) list :+= ("D00010002 ", 1)
    else if (clienttype == 3) list :+= ("D00010003 ", 1)
    else list :+= ("D00010004", 1)

    //获取设备联网方式
    val networkmannername = row.getAs[String]("networkmannername")
    if (StringUtils.isNotBlank(networkmannername)) {
      if (networkmannername.equals("WIFI")) list :+= ("D00020001", 1)
      else if (networkmannername.equals("4G")) list :+= ("D00020002", 1)
      else if (networkmannername.equals("3G")) list :+= ("D00020003", 1)
      else if (networkmannername.equals("2G")) list :+= ("D00020004", 1)
      else list :+= ("D00020005", 1)
    }

    //获取运营商
    val ispname = row.getAs[String]("ispname")
    if (StringUtils.isNotBlank(ispname)) {
      if (ispname.equals("移动")) list :+= ("D00030001", 1)
      else if (ispname.equals("联通")) list :+= ("D00030002", 1)
      else if (ispname.equals("电信")) list :+= ("D00030003", 1)
      else list :+= ("D00030004", 1)
    }


    list
  }
}
