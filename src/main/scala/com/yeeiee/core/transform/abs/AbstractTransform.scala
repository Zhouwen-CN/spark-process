package com.yeeiee.core.transform.abs

import com.yeeiee.beans.{Logging, TransformAttribute}
import com.yeeiee.core.env.ContextManager
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

/**
 * @Author: chen
 * @Date: 2023/7/4
 * @Desc:
 */
abstract class AbstractTransform extends TransformAttribute with Transform with Logging {

  protected def realRun(context: ContextManager, operands: List[DataFrame]): DataFrame

  override def run(context: ContextManager, dfs: List[DataFrame]): List[DataFrame] = {
    // 操作数列表,没有给定取默认
    val realOss: List[Int] = if (getOss.nonEmpty) getOss else getOssDefault

    // 取出指定的操作数个数
    var doTaskOss: ListBuffer[Int] = ListBuffer[Int](realOss: _*)
    doTaskOss = if (getOsn > 0) doTaskOss.take(getOsn) else doTaskOss

    // 如果操作数为空,则取所有的操作数
    if (doTaskOss.isEmpty) {
      doTaskOss = ListBuffer(dfs.indices.toList: _*)
    }

    // 执行转换
    val doTaskDf: ListBuffer[DataFrame] = doTaskOss.map(dfs(_))
    val result: DataFrame = realRun(context, doTaskDf.toList)

    // 替换下标
    val replaceOs: Int = doTaskOss.head
    doTaskOss.remove(0)
    // 删除操作数列表
    val deleteOss: ListBuffer[Int] = doTaskOss

    val results: ListBuffer[DataFrame] = ListBuffer.empty[DataFrame]
    for (idx <- dfs.indices) {
      if (replaceOs.equals(idx)) {
        results.append(result)
      } else if (!deleteOss.contains(idx)) {
        results.append(dfs(idx))
      }
    }

    results.toList
  }
}