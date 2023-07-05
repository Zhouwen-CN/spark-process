package com.yeeiee.core.sink

import com.yeeiee.core.env.ContextManager
import org.apache.spark.sql.DataFrame

/**
 * @Author: chen
 * @Date: 2023/7/3
 * @Desc:
 */
trait Sink {
  def run(context: ContextManager, df: DataFrame): Unit
}
