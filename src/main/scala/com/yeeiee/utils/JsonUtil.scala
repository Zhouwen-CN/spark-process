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
   * 如果是string直接返回,别的类型转成json返回
   *
   * @param jsonObj
   * @return
   */
  def toJsonStr(jsonObj: Any): String = {
    if (jsonObj.isInstanceOf[String]) {
      jsonObj.toString
    } else {
      MAPPER.writeValueAsString(jsonObj)
    }
  }

  def toJsonMap[K, V](jsonObj: Any, keyType: Class[K], valueType: Class[V]): Map[K, V] = {
    toJsonObj(toJsonStr(jsonObj), Map[K, V]().getClass)
  }

  def toJsonObj[T](jsonObj: Any, valueType: Class[T]): T = {
    MAPPER.readValue(toJsonStr(jsonObj), valueType)
  }

  def toJsonObj(jsonObj: Any, classFullName: String): Any = {
    toJsonObj(jsonObj, Class.forName(classFullName))
  }
}
