package com.Utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 标签工具类
  */
object TagUtils {

  // 过滤需要的字段
  val OneUserId=
    """
      | imei !='' or mac !='' or openudid !='' or androidid !='' or idfa !='' or
      | imeimd5 !='' or macmd5 !='' or openudidmd5 !='' or androididmd5 !='' or idfamd5 !='' or
      | imeisha1 !='' or macsha1 !='' or openudidsha1 !='' or androididsha1 !='' or idfasha1 !=''
    """.stripMargin

  // 取出唯一不为空的Id
  def getOneUserId(row: Row): String = {
    row match {
      case v if StringUtils.isNotBlank(v.getAs[String]("imei")) => "IMEA: " + v.getAs[String]("imei")
      case v if StringUtils.isNotBlank(v.getAs[String]("mac")) => "MAC: " + v.getAs[String]("mac")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudid")) => "OU: " + v.getAs[String]("openudid")
      case v if StringUtils.isNotBlank(v.getAs[String]("androidid")) => "AI: " + v.getAs[String]("androidid")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfa")) => "IDFA: " + v.getAs[String]("idfa")
      case v if StringUtils.isNotBlank(v.getAs[String]("imeimd5")) => "IMEIMD5: " + v.getAs[String]("imeimd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("macmd5")) => "MACMD5: " + v.getAs[String]("macmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidmd5")) => "OUMD5: " + v.getAs[String]("openudidmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididmd5")) => "AIMD5: " + v.getAs[String]("androididmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfamd5")) => "IDFAMD5: " + v.getAs[String]("idfamd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("imeisha1")) => "IMEISHA1: " + v.getAs[String]("imeisha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("macsha1")) => "MACSHA1: " + v.getAs[String]("macsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidsha1")) => "OUSHA1: " + v.getAs[String]("openudidsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididsha1")) => "AISHA1: " + v.getAs[String]("androididsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfasha1")) => "IDFASHA1: " + v.getAs[String]("idfasha1")
    }
  }
}
