package com.yeeiee.core.factory

import com.yeeiee.beans.{Attribute, Logging}
import com.yeeiee.constants.StringConstant
import com.yeeiee.utils.JsonUtil

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
abstract class AbstractFactory[T] extends Factory[T] with Logging {

  def build(material: Any): T = {
    val clazz: String = JsonUtil.toJsonObj(material, classOf[Attribute]).getClazz

    if (clazz.isEmpty) {
      throw new Exception("the work clazz must be not empty ...")
    }

    val correctClazz: String = getCorrectClazz(clazz)
    logInfo(s"abstractFactory will build clazz [ $correctClazz ]")
    JsonUtil.toJsonObj(material, correctClazz).asInstanceOf[T]
  }

  protected def getPackagePrefixes: List[String]

  private def getCorrectClazz(clazz: String): String = {
    if (clazz.contains(StringConstant.POINT)) {
      clazz
    } else {
      searchClazz(clazz, getPackagePrefixes)
    }
  }

  private def searchClazz(clazz: String, prefixes: List[String]): String = {
    val findClass: List[String] = prefixes.map(_ + clazz).filter(checkClazz)

    if (findClass.isEmpty) {
      throw new Exception(s"abstractFactory find no clazz [ $clazz ]")
    }

    findClass.head
  }

  private def checkClazz(clazz: String): Boolean = {
    try {
      Class.forName(clazz)
      true
    } catch {
      case e: Exception => false
    }
  }
}
