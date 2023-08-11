package com.yeeiee.utils

import com.yeeiee.constants.StringConstant

/**
 * @Author: chen
 * @Date: 2023/7/4
 * @Desc:
 */
object SqlUtil {
  def partition(partition: String): String = {
    if (Option(partition).getOrElse("").nonEmpty) {
      s"partition($partition) "
    } else {
      StringConstant.EMPTY
    }
  }

  def insert(mode: String, out: String): String = {
    s"insert $mode table ${checkTable(out)} "
  }

  def select(exprs: List[String] = List.empty[String]): String = {
    s"select ${columns(exprs)}"
  }

  def columns(exprs: List[String]): String = {
    if (Option(exprs).getOrElse(List.empty[String]).isEmpty) {
      StringConstant.ASTERISK
    } else {
      exprs.mkString(StringConstant.COMMA + StringConstant.EMPTY)
    }
  }

  def from(table: String, check: Boolean = true): String = {
    val realTable: String = Option(table).getOrElse(StringConstant.EMPTY)
    if (check) {
      s"from ${checkTable(realTable)}"
    } else {
      s"from $realTable"
    }
  }

  def where(filter: String): String = {
    val realFilter: String = Option(filter).getOrElse(StringConstant.EMPTY)
    if (realFilter.isEmpty) {
      StringConstant.EMPTY
    } else {
      s"where $realFilter"
    }
  }

  def desc(table: String): String = {
    s"desc ${checkTable(table)}"
  }

  def checkTable(table: String): String = {
    if (table.contains(StringConstant.POINT)) {
      table
    } else {
      throw new Exception(s"table name must be warehouse.table: [ $table ] ...")
    }
  }
}
