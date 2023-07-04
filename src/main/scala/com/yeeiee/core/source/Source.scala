package com.yeeiee.core.source

import com.yeeiee.core.env.ContextManager

/**
 * @Author: chen
 * @Date: 2023/7/3
 * @Desc:
 */
trait Source {
  def run(context: ContextManager): String
}
