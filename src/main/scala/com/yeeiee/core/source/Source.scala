package com.yeeiee.core.source

import com.yeeiee.core.env.ContextManager

trait Source {
  def run(context: ContextManager): String
}
