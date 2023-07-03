package com.yeeiee.core.env

import com.yeeiee.core.params.ParamManager

case class ContextManager(
  session: SessionProxy,
  config: ConfigManager,
  param: ParamManager
)
