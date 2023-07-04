package com.yeeiee.core.env

import com.yeeiee.beans.Logging
import com.yeeiee.utils.JsonUtil

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
class CommonConfigManager extends ConfigManager with Logging {
  private var elements: Map[String, String] = _

  override def parse(material: Any): ConfigManager = {
    elements = JsonUtil.toJsonMap(material, classOf[String], classOf[String])
    this
  }

  override def foreach(f: ((String, String)) => Unit): Unit = {
    Option(elements) match {
      case None => logInfo("config elements is none ...")
      case _ => elements.foreach(f)
    }
  }
}
