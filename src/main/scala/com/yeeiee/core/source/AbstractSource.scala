package com.yeeiee.core.source

import com.yeeiee.beans.{Attribute, Logging}
import com.yeeiee.constants.StringConstant
import com.yeeiee.core.env.ContextManager

/**
 * @Author: chen
 * @Date: 2023/7/3
 * @Desc:
 */
abstract class AbstractSource(out: String) extends Attribute with Source with Logging{
  def confirmRun(context: ContextManager) : String

  /**
   * 检查out是否为空
   * @param context
   * @return
   */
  override def run(context: ContextManager): String = {
    val realOut: String = Option(out).getOrElse(StringConstant.EMPTY)
    if (realOut.nonEmpty) {
      confirmRun(context)
    }else{
      throw new Exception("source out table name must be not empty ...")
    }
  }
}
