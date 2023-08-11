package com.yeeiee.core.source

import com.yeeiee.core.env.ContextManager
import org.apache.spark.sql.DataFrame

/**
 * @Author: chen
 * @Date: 2023/7/4
 * @Desc:
 */
class HiveSource(
    exprs: List[String],
    table: String,
    condition: String,
    ignores: List[String]) extends AbstractSource {

  override def run(context: ContextManager): DataFrame = {
    context.session
      .createTable(exprs, table, condition)
      .drop(
        Option(ignores).getOrElse(List.empty[String]): _*)
  }
}
