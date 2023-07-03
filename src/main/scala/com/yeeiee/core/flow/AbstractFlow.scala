package com.yeeiee.core.flow

import com.yeeiee.beans.{Attribute, Logging}
import com.yeeiee.constants.StringConstant
import com.yeeiee.core.env.{CommonConfigManager, ConfigManager, ContextManager, SessionProxy}
import com.yeeiee.core.params.ParamManager

abstract class AbstractFlow(config: Any) extends Attribute with Flow with Logging {

  protected def createContext(paramManager: ParamManager): ContextManager = {
    val configManager: ConfigManager = new CommonConfigManager()
    configManager.parse(config)

    val taskName: String = paramManager.get(StringConstant.TASK_NAME)
    val taskTime: String = paramManager.get(StringConstant.TASK_TIME)

    val appName: String = s"$taskName-$taskTime"

    val sessionProxy: SessionProxy = new SessionProxy(appName, configManager, paramManager)

    ContextManager(sessionProxy, configManager, paramManager)
  }

}
