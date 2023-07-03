package com.yeeiee.core.factory

import com.yeeiee.beans.PackagePrefix
import com.yeeiee.core.source.Source

object SourceFactory extends AbstractFactory[Source] {
  override protected def getPackagePrefixes: List[String] = List(PackagePrefix.SOURCE.toString)
}


