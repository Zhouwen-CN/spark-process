package com.yeeiee.core.factory

trait Factory[T] {
  def build(material: Any): T
}
