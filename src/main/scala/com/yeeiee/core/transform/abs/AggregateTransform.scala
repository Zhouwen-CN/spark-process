package com.yeeiee.core.transform.abs

import com.yeeiee.beans.AggElement
import com.yeeiee.constants.NumberConstant
import com.yeeiee.core.env.ContextManager
import org.apache.spark.sql.{Column, DataFrame, Dataset, RelationalGroupedDataset}
import org.apache.spark.sql.functions.expr

/**
 * @Author: chen
 * @Date: 2023/7/5
 * @Desc:
 */
abstract class AggregateTransform(groups: List[String], aggs: List[AggElement])
  extends SingleOperandTransform {

  override protected def realRun(context: ContextManager, operands: List[DataFrame]): DataFrame = {
    val original: DataFrame = operands(NumberConstant.NUMBER_0)

    val groupList: List[String] = Option(groups).getOrElse(List.empty[String])
    val aggList: List[AggElement] = Option(aggs).getOrElse(List.empty[AggElement])

    // 如果groups和aggs都为空，则无法进行聚合计算
    if (groupList.isEmpty && aggList.isEmpty) {
      throw new Exception("you need to configure groups and aggs ...")
    }

    val groupColumn: List[Column] = groupList.map(group => expr(group))
    val relationalSet: RelationalGroupedDataset = getRGDataset(original, groupColumn)

    // 如果aggList为空，说明没有配置聚合函数
    if (aggList.isEmpty) {
      relationalSet.agg(Map.empty[String, String])
    } else {
      val columnOne: Column = getAggColumn(aggList(NumberConstant.NUMBER_0))
      val columns: List[Column] = aggList.drop(NumberConstant.NUMBER_1).map(getAggColumn)
      relationalSet.agg(columnOne, columns: _*)
    }
  }

  private def getAggColumn(agg: AggElement): Column = expr(agg.func).name(agg.name)

  protected def getRGDataset[T](
      original: Dataset[T],
      groupColumn: List[Column]): RelationalGroupedDataset
}
