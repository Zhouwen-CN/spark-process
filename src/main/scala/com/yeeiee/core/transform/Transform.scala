package com.yeeiee.core.transform

import com.yeeiee.core.env.ContextManager

/**
 * @Author: chen
 * @Date: 2023/7/3
 * @Desc:
 */
trait Transform {
  def run(context: ContextManager, tableNames: List[String]): List[String]
}
