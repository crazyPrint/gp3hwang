package com.Tags

import java.util

import com.Utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.{Jedis, JedisPool}

object TagsAppRedis extends Tag  with Serializable{

  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]

    // 获取	App 名称
    var appnameKey=""
    val appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")

    val pool = new JedisPool("hadoop01", 6379)
    val jedis: Jedis = pool.getResource


    val appNameFromRedis: String = jedis.hget("dict_appname",appid)
    jedis.close()
    pool.close()

    //按照条件获取数据
    if(StringUtils.isNotBlank(appname)){
      list:+=(appname,1)
    }else if(StringUtils.isNotBlank(appNameFromRedis)){
      list:+=(appNameFromRedis,1)
    }
    list
  }
}


//    val mapdic: util.Map[String, String] = jedis.hgetAll("dict_appname")
//    jedis.close()
//    if (!appname.equals("")) {
//      appnameKey = appname
//    } else {
//      appnameKey = mapdic.getOrDefault(appid,"其他")
//    }
//    if (StringUtils.isNotBlank(appnameKey)) {
//      list :+= ("APP" + appnameKey, 1)
//    }
//    list