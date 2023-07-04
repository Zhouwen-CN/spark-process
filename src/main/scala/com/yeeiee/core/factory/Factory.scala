package com.yeeiee.core.factory

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
trait Factory[T] {
  def build(material: Any): T
}
