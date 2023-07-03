package com.yeeiee.core.flow

import com.yeeiee.core.params.ParamManager

trait Flow {
  def run(param: ParamManager): Unit
}