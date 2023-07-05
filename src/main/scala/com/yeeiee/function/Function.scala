package com.yeeiee.function

import org.apache.spark.sql.expressions.UserDefinedFunction

/**
 * @Author: chen
 * @Date: 2023/7/5
 * @Desc:
 */
trait Function extends Serializable {
  def define: UserDefinedFunction
}
