package com.yeeiee.core.source

import com.yeeiee.core.env.ContextManager
import org.apache.spark.sql.DataFrame

/**
 * @Author: chen
 * @Date: 2023/7/3
 * @Desc:
 */
trait Source {
  def run(context: ContextManager): DataFrame
}
