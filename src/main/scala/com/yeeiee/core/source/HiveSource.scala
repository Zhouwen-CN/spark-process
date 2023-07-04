package com.yeeiee.core.source

import com.yeeiee.core.env.ContextManager

/**
 * @Author: chen
 * @Date: 2023/7/4
 * @Desc:
 */
class HiveSource(
                  out: String,
                  exprs: List[String],
                  table: String,
                  condition: String,
                  ignores: List[String]) extends AbstractSource(out) {
  override def confirmRun(context: ContextManager): String = {
    context.session
      .registerTable(exprs, table, condition)
      .drop(
        Option(ignores).getOrElse(List.empty[String]): _*
      )
      .createOrReplaceTempView(out)

    out
  }
}
