package com.yeeiee.core.transform

import com.yeeiee.constants.{NumberConstant, StringConstant}
import com.yeeiee.core.env.ContextManager
import com.yeeiee.core.transform.abs.SingleOperandTransform
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

/**
 * @Author: chen
 * @Date: 2023/7/5
 * @Desc:
 */
class WhereTransform(condition: String) extends SingleOperandTransform {

  override protected def realRun(context: ContextManager, operands: List[DataFrame]): DataFrame = {
    val operand: DataFrame = operands(NumberConstant.NUMBER_0)

    val onCondition: String = Option(condition).getOrElse(StringConstant.EMPTY)
    if (onCondition.isEmpty) {
      throw new Exception("the where transform condition must be not empty ...")
    }

    operand.where(expr(condition))
  }
}
