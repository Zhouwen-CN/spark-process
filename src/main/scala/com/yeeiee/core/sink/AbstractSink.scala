package com.yeeiee.core.sink

import com.yeeiee.beans.{Attribute, Logging}
import com.yeeiee.constants.StringConstant
import com.yeeiee.core.env.ContextManager
import com.yeeiee.utils.StringUtil
import org.apache.spark.sql.DataFrame

/**
 * @Author: chen
 * @Date: 2023/7/3
 * @Desc:
 */
abstract class AbstractSink extends Attribute with Sink with Logging {
  /**
   * 检查任务名称和sink表名是否一致
   *
   * @param context
   * @return
   */
  private def confirm(context: ContextManager): Boolean = {
    val taskName: String = context.param.get(StringConstant.TASK_NAME)

    // table_insert-default-student1
    val taskFeature: String = StringUtil.getWarehouseAndTableName(taskName)
    val sinkFeature: String = getSinkFeature
    logInfo(s"abstractSink will confirm feature: $taskFeature => $sinkFeature ...")
    taskFeature.equals(sinkFeature)
  }

  protected def getSinkFeature: String

  protected def confirmedRun(context: ContextManager, df: DataFrame): Unit

  override def run(context: ContextManager, df: DataFrame): Unit = {
    if (confirm(context)) {
      confirmedRun(context, df)
    } else {
      throw new Exception("task name not equal to sink table name ...")
    }
  }
}
