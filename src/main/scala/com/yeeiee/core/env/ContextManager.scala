package com.yeeiee.core.env

import com.yeeiee.core.params.ParamManager

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
case class ContextManager(
    session: SessionProxy,
    config: ConfigManager,
    param: ParamManager)
