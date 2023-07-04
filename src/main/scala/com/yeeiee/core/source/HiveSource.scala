package com.yeeiee.core.source

import com.yeeiee.core.env.ContextManager
import org.apache.spark.sql.DataFrame

class HiveSource(
                  out: String,
                  exprs: List[String],
                  table: String,
                  condition: String,
                  ignores: List[String]) extends AbstractSource {

  override def run(context: ContextManager): String = {

    context.session
      .registerTable(exprs, table, condition)
      .drop(
        Option(ignores).getOrElse(List.empty[String]): _*
      )
      .createOrReplaceTempView(out)

    out
  }
}
