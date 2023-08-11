package com.yeeiee.core.factory

import com.yeeiee.beans.PackagePrefix
import com.yeeiee.core.transform.abs.Transform

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
object TransformFactory extends AbstractFactory[Transform] {
  override protected def getPackagePrefixes: List[String] = List(PackagePrefix.TRANSFORM.toString)
}
