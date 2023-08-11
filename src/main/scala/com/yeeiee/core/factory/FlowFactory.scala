package com.yeeiee.core.factory

import com.yeeiee.beans.PackagePrefix
import com.yeeiee.core.flow.Flow

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
object FlowFactory extends AbstractFactory[Flow] {
  override protected def getPackagePrefixes: List[String] = List(PackagePrefix.FLOW.toString)
}
