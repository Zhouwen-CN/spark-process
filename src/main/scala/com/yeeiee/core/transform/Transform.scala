package com.yeeiee.core.transform

import com.yeeiee.core.env.ContextManager

trait Transform {
  def run(context: ContextManager, tableNames: List[String]): List[String]
}
