package com.yeeiee.core.transform.abs

import com.yeeiee.beans.{Attribute, Logging}
import com.yeeiee.constants.StringConstant
import com.yeeiee.core.env.ContextManager

abstract class AbstractTransform extends Attribute with Transform with Logging {

  def getIns: List[String];

  def confirm(tableNames: List[String]): Boolean = {
    val needTableNames: List[String] = getIns
    tableNames.intersect(needTableNames) == needTableNames
  }

  def confirmRun(manager: ContextManager, strings: List[String]): List[String]

  override def run(context: ContextManager, tableNames: List[String]): List[String] = {
    if (confirm(tableNames)) {
      confirmRun(context: ContextManager, tableNames: List[String])
    } else {
      throw new Exception(s"can not resolve tableNames [ ${getIns.mkString(StringConstant.COMMA)} ] in this list [ ${tableNames.mkString(StringConstant.COMMA)}] ...")
    }
  }
}
