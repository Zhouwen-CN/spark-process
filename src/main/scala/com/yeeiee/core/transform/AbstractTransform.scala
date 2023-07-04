package com.yeeiee.core.transform

import com.yeeiee.beans.{Attribute, Logging}
import com.yeeiee.constants.StringConstant
import com.yeeiee.core.env.ContextManager

abstract class AbstractTransform(ins: List[String]) extends Attribute with Transform with Logging {

  def checkTableSize:Int

  /**
   * 检查ins是否为空,ins长度时候小于最小需要长度
   * @param tableNames
   * @return
   */
  def confirm(tableNames: List[String]): Boolean = {
    val realIns: List[String] = Option(ins).getOrElse(List.empty[String])

    if (realIns.isEmpty) {
      throw new Exception(s"the transform ins table must be not empty ...")
    }

    if (realIns.size < checkTableSize) {
      throw new Exception(s"then transform need table size [ $checkTableSize ] but give ins size [ ${realIns.size} ] ...")
    }

    tableNames.intersect(realIns) == realIns
  }

  def confirmRun(manager: ContextManager, tableNames: List[String]): List[String]

  override def run(context: ContextManager, tableNames: List[String]): List[String] = {
    if (confirm(tableNames)) {
      confirmRun(context: ContextManager, tableNames: List[String])
    } else {
      throw new Exception(s"can not resolve tableNames [ ${ins.mkString(StringConstant.COMMA)} ] in this list [ ${tableNames.mkString(StringConstant.COMMA)}] ...")
    }
  }
}
