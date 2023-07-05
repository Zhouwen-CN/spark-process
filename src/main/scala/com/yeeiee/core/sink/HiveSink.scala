package com.yeeiee.core.sink

import com.yeeiee.constants.StringConstant
import com.yeeiee.core.env.ContextManager
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

/**
 * @Author: chen
 * @Date: 2023/7/3
 * @Desc:
 */
class HiveSink(
                mode: String,
                table: String,
                partition: String,
                columns: List[String] = List.empty[String]
              ) extends AbstractSink {
  override protected def getSinkFeature: String = table

  override protected def confirmedRun(context: ContextManager, df: DataFrame): Unit = {
    val outputColumns: List[String] = getOutputColumns(context)

    val output: DataFrame = df.selectExpr(outputColumns: _*)

    context.session.insertTable(output, mode, table, partition)
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
