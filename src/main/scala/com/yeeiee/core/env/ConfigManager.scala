package com.yeeiee.core.env

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
trait ConfigManager {
  def parse(material: Any): ConfigManager

  def foreach(f: ((String, String)) => Unit): Unit
}
