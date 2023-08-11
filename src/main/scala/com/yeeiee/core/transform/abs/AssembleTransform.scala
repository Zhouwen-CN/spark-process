package com.yeeiee.core.transform.abs

import com.yeeiee.constants.NumberConstant
import com.yeeiee.core.env.ContextManager
import org.apache.spark.sql.DataFrame

/**
 * @Author: chen
 * @Date: 2023/7/5
 * @Desc: 2.4有不去重的交集和差集,目前交集差集只能去重,而union默认不去重
 *        unionByName按照字段join,但是交集差集是按字段顺序的
 *        这里统一处理
 */
abstract class AssembleTransform(alignment: Boolean, distinct: Boolean)
  extends MultipleOperandTransform {

  override protected def realRun(context: ContextManager, operands: List[DataFrame]): DataFrame = {
    // 检查传入的表是否超过两个,小于两个无法union
    if (operands.size < NumberConstant.NUMBER_2) {
      throw new Exception("you should configure more than or equal to 2 tables ...")
    }

    // 循环所有DataFrame,将他们关联
    val head: DataFrame = operands(NumberConstant.NUMBER_0)

    // 获取第一个DF的字段集
    val headColumns: Array[String] = head.columns

    for (i <- NumberConstant.NUMBER_1 until operands.length) {

      val operand: DataFrame =
        if (Option(alignment).getOrElse(NumberConstant.TRUE)) {
          operands(i).selectExpr(headColumns: _*)
        } else {
          operands(i)
        }

      assemble(head, operand)
    }

    // 根据distinct判断是否需要对重复列进行去重
    if (Option(distinct).getOrElse(NumberConstant.FALSE)) {
      head
    } else {
      head.dropDuplicates()
    }
  }

  protected def assemble(head: DataFrame, other: DataFrame): DataFrame
}
