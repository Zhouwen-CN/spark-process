package com.yeeiee.core.sink

import com.yeeiee.constants.StringConstant
import com.yeeiee.core.env.ContextManager

import scala.collection.mutable.ListBuffer

class HiveSink(
                in: String,
                mode: String,
                table: String,
                partition: String,
                columns: List[String] = List.empty[String]
              ) extends AbstractSink {
  override protected def getSinkFeature: String = table

  override protected def confirmedRun(context: ContextManager, tableName: String): Unit = {

    val outputColumns: List[String] = getOutputColumns(context)

    context.session.getTable(tableName).selectExpr(outputColumns: _*).createOrReplaceTempView(in)

    context.session.insertTable(in, mode, table, partition)
  }

  private def getOutputColumns(context: ContextManager): List[String] = {
    if (Option(columns).getOrElse(List.empty[String]).nonEmpty) {
      columns
    } else {
      val desc: List[String] = context.session
        .descTable(table)
        .collect()
        .map(item => item.getAs[String](0))
        .toList

      val columnBuffer: ListBuffer[String] = ListBuffer.empty[String]
      var switch: Boolean = false
      desc.foreach {
        item =>
          if (item.startsWith(StringConstant.POUND_KEY)) {
            switch = true
          } else {
            if (switch.equals(false)) {
              columnBuffer.append(item)
            } else {
              columnBuffer --= columnBuffer.filter(_.equals(item))
            }
          }
      }
      columnBuffer.toList
    }
  }
}
