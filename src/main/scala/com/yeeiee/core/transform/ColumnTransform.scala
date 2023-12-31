package com.yeeiee.core.transform

import com.yeeiee.beans.ColumnElement
import com.yeeiee.core.env.ContextManager
import com.yeeiee.core.transform.abs.SingleOperandTransform
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

/**
 * @Author: chen
 * @Date: 2023/7/5
 * @Desc:
 */
class ColumnTransform(columns: List[ColumnElement]) extends SingleOperandTransform {

  override protected def realRun(context: ContextManager, operands: List[DataFrame]): DataFrame = {
    if (Option(columns).getOrElse(List.empty[ColumnElement]).isEmpty) {
      throw new Exception("usage columnTransform but columns is empty ...")
    }
    var operand: DataFrame = operands.head

    columns.foreach(e => {
      operand = operand.withColumn(e.name, expr(e.expr))
    })

    operand
  }
}
