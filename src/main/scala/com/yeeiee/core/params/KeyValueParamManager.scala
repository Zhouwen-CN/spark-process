package com.yeeiee.core.params

import com.yeeiee.constants.StringConstant

class KeyValueParamManager extends ParamManager {
  private var elements: Map[String, String] = _

  override def parse(params: List[String]): ParamManager = {
    elements = params.map(e => {
      val arr: Array[String] = e.split(StringConstant.EQUAL)
      (arr(0), arr(1))
    }).toMap

    this
  }

  override def getValue(key: String): Option[String] = {
    elements.get(key)
  }

  override def foreach(f: ((String, String)) => Unit): Unit = {
    elements.foreach(f)
  }
}
