package com.yeeiee.core.source

import com.yeeiee.core.env.ContextManager
import org.apache.spark.sql.DataFrame

class HiveSource(
                  exprs: List[String],
                  table: String,
                  condition: Any,
                  ignores: List[String]
                ) extends AbstractSource {
  override def run(
                    context: ContextManager
                  ): DataFrame = {

    val filter = if (Option(condition).isDefined) {
      ConditionElement.get(condition).expr.sql
    } else {
      StringConstant.EMPTY
    }

    context.session
      .createDataFrame(exprs, table, filter)
      .drop(
        Option(ignores).getOrElse(List.empty[String]): _*
      )
  }
}
