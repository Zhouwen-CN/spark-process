package com.yeeiee.core.sink

import com.yeeiee.core.env.ContextManager

/**
 * @Author: chen
 * @Date: 2023/7/3
 * @Desc:
 */
trait Sink {
  def run(context: ContextManager, tableName: String): Unit
}
