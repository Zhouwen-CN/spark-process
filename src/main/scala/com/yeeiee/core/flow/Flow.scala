package com.yeeiee.core.flow

import com.yeeiee.core.params.ParamManager

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
trait Flow {
  def run(param: ParamManager): Unit
}