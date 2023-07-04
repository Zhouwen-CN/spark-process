package com.yeeiee.beans

import com.yeeiee.constants.StringConstant

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
object PackagePrefix extends Enumeration {
  type PackagePrefix = Value

  val CORE = Value("com.yeeiee.core.")
  val FLOW = Value(s"${CORE.toString}flow${StringConstant.POINT}")
  val SOURCE = Value(s"${CORE.toString}source${StringConstant.POINT}")
  val TRANSFORM = Value(s"${CORE.toString}transform${StringConstant.POINT}")
  val SINK = Value(s"${CORE.toString}sink${StringConstant.POINT}")
}
