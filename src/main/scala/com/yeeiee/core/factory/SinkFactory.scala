package com.yeeiee.core.factory

import com.yeeiee.beans.PackagePrefix
import com.yeeiee.core.sink.Sink

object SinkFactory extends AbstractFactory[Sink] {
  override protected def getPackagePrefixes: List[String] = List(PackagePrefix.SINK.toString)
}


