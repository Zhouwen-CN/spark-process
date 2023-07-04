package com.yeeiee.core.factory

import com.yeeiee.beans.PackagePrefix
import com.yeeiee.core.source.Source

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
object SourceFactory extends AbstractFactory[Source] {
  override protected def getPackagePrefixes: List[String] = List(PackagePrefix.SOURCE.toString)
}


