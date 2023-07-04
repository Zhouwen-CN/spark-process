package com.yeeiee.core.params

import com.yeeiee.constants.StringConstant

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
trait ParamManager {
  def parse(params: List[String]): ParamManager

  protected def getValue(key: String): Option[String]

  def foreach(f: ((String, String)) => Unit): Unit


  def wrapParamKey(key: String): String = {
    StringConstant.DOLLAR + StringConstant.LEFT_BRACE + key + StringConstant.RIGHT_BRACE
  }

  def get(key: String): String = {
    getValue(key) match {
      case Some(item) => item
      case None =>
        throw new Exception(s"param manager get param error: [ $key ]")
    }
  }

  def getOrElse(key: String, default: String): String = {
    getValue(key) match {
      case Some(item) => item
      case None => default
    }
  }
}
