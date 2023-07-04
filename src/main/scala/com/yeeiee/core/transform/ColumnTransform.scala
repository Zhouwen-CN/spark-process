package com.yeeiee.core.transform

import com.yeeiee.beans.ColumnElement
import com.yeeiee.core.env.ContextManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

import scala.collection.mutable

/**
 * @Author: chen
 * @Date: 2023/7/4 16:01
 * @Desc:
 */

class ColumnTransform(ins: List[String], out: String, columns: List[ColumnElement]) extends AbstractTransform(ins) {
  override def checkTableSize: Int = 1

  override def confirmRun(manager: ContextManager, tableNames: List[String]): List[String] = {

    val in: String = ins.head
    // 如果没有给定out,那就取in的值
    val ou: String = Option(out).getOrElse(ins.head)
    val df: DataFrame = manager.session.getTable(in)

    if (Option(columns).isEmpty) {
      throw new Exception("usage columnTransform but columns is empty ...")
    }

    columns.foreach(e=>{
      df.withColumn(e.name,expr(e.expr)).createOrReplaceTempView(ou)
    })

    val buffer: mutable.Buffer[String] = tableNames.toBuffer
    buffer.remove(buffer.indexOf(in))

    buffer.append(ou)

    buffer.toList
  }
}
