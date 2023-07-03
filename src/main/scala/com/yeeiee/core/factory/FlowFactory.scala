package com.yeeiee.core.factory

import com.yeeiee.beans.PackagePrefix
import com.yeeiee.core.flow.Flow

object FlowFactory extends AbstractFactory[Flow] {
  override protected def getPackagePrefixes: List[String] = List(PackagePrefix.FLOW.toString)
}