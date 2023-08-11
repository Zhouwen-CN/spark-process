package com.yeeiee.core.factory

import com.yeeiee.beans.PackagePrefix
import com.yeeiee.core.sink.Sink

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
object SinkFactory extends AbstractFactory[Sink] {
  override protected def getPackagePrefixes: List[String] = List(PackagePrefix.SINK.toString)
}
