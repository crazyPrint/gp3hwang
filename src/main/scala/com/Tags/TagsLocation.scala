package com.Tags

import com.Utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsLocation extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    //获取操作系统
    val provincename = row.getAs[String]("provincename")
    val cityname = row.getAs[String]("cityname")

    if (StringUtils.isNotBlank(provincename)&&StringUtils.isNotBlank(cityname)){
      list :+= ("ZP"+provincename, 1)
      list :+= ("ZC"+cityname, 1)
    }
    list
  }
}
