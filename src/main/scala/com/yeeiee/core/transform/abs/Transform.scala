package com.yeeiee.core.transform.abs

import com.yeeiee.core.env.ContextManager
import org.apache.spark.sql.DataFrame

/**
 * @Author: chen
 * @Date: 2023/7/3
 * @Desc:
 */
trait Transform {
  def run(context: ContextManager, dfs: List[DataFrame]): List[DataFrame]
}
