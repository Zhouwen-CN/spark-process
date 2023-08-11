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
class JsonGetTransform(column: String, path: String, name: String) extends SingleOperandTransform {

  override protected def realRun(context: ContextManager, operands: List[DataFrame]): DataFrame = {
    val original: DataFrame = operands(NumberConstant.NUMBER_0)

    if (Option(path).getOrElse(StringConstant.EMPTY).isEmpty) {
      throw new Exception("the json get transform you should configure path value ...")
    }

    val realName: String =
      if (Option(name).getOrElse(StringConstant.EMPTY).nonEmpty) name else column
    original.withColumn(
      realName,
      functions.get_json_object(original(column), path))
  }
}
