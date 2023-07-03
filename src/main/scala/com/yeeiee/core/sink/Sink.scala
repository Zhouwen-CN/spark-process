package com.yeeiee.core.sink

import com.yeeiee.core.env.ContextManager

trait Sink {
  def run(context: ContextManager, tableName: String): Unit
}
