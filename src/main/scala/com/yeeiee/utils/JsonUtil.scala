package com.yeeiee.utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
object JsonUtil extends Serializable {
  private val MAPPER = new ObjectMapper

  MAPPER.registerModule(DefaultScalaModule)

  /**
   * 当解析到不存在的属性时,忽略报错
   */
  MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  /**
   * 节点属性会被解析成tuple,需要转成json string
   *
   * @param jsonObj
   * @return
   */
  def toJsonStr(jsonObj: Any): String = MAPPER.writeValueAsString(jsonObj)


  /*  def toJsonObj[T](jsonStr: String): Any = {
      toJsonObj(jsonStr, classOf[Any])
    }*/

  def toJsonMap[K, V](jsonObj: Any, keyType: Class[K], valueType: Class[V]): Map[K, V] = {
    val jsonStr: String = toJsonStr(jsonObj)
    toJsonObj(jsonStr, Map[K, V]().getClass)
  }

  def toJsonObj[T](jsonObj: Any, valueType: Class[T]): T = {
    if (jsonObj.isInstanceOf[String]) {
      MAPPER.readValue(jsonObj.toString, valueType)
    } else {
      MAPPER.readValue(toJsonStr(jsonObj), valueType)
    }
  }

  def toJsonObj(jsonObj: Any, classFullName: String): Any = {
    toJsonObj(jsonObj, Class.forName(classFullName))
  }
}
