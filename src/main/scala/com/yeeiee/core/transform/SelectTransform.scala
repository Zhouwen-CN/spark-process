package com.yeeiee.core.transform

import com.yeeiee.constants.{NumberConstant, StringConstant}
import com.yeeiee.core.env.ContextManager
import com.yeeiee.core.transform.abs.SingleOperandTransform
import org.apache.spark.sql.DataFrame

/**
 * @Author: chen
 * @Date: 2023/7/5
 * @Desc:
 */
class SelectTransform(matched: List[String], unmatched: List[String])
  extends SingleOperandTransform {

  override protected def realRun(context: ContextManager, operands: List[DataFrame]): DataFrame = {
    val operand: DataFrame = operands(NumberConstant.NUMBER_0)

    val realMatched: List[String] = Option(matched).getOrElse(List(StringConstant.ASTERISK))
    val realUnmatched: List[String] = Option(unmatched).getOrElse(List.empty[String])

    operand.selectExpr(realMatched: _*).drop(realUnmatched: _*)
  }
}
