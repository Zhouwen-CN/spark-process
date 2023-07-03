package com.yeeiee.core.factory

import com.yeeiee.beans.PackagePrefix
import com.yeeiee.core.transform.Transform

object TransformFactory extends AbstractFactory[Transform] {
  override protected def getPackagePrefixes: List[String] = List(PackagePrefix.TRANSFORM.toString)
}


