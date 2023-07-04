package com.yeeiee.utils

import com.yeeiee.constants.{NumberConstant, StringConstant}


/**
 * @Author: chen
 * @Date: 2023/7/4
 * @Desc:
 */
object StringUtil {
  def getWarehouseAndTableName(taskName:String):String={
    val splitArr: Array[String] = taskName.split(StringConstant.STRIKETHROUGH)
    s"${splitArr(NumberConstant.NUMBER_1)}.${splitArr(NumberConstant.NUMBER_2)}"
  }
}