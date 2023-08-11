package com.yeeiee.core.transform

import com.yeeiee.constants.{NumberConstant, StringConstant}
import com.yeeiee.core.env.ContextManager
import com.yeeiee.core.transform.abs.SingleOperandTransform
import org.apache.spark.sql.{functions, DataFrame}

/**
 * @Author: chen
 * @Date: 2023/7/5
 * @Desc:
 */
class ExplodeTransform(column: String, name: String) extends SingleOperandTransform {

  override protected def realRun(context: ContextManager, operands: List[DataFrame]): DataFrame = {
    val operand: DataFrame = operands(NumberConstant.NUMBER_0)
    val realName: String =
      if (Option(name).getOrElse(StringConstant.EMPTY).nonEmpty) name else column

    operand.withColumn(realName, functions.explode(functions.expr(column)))
  }
}
