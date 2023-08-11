package com.yeeiee.core.transform

import com.yeeiee.beans.AggElement
import com.yeeiee.core.transform.abs.AggregateTransform
import org.apache.spark.sql.{Column, Dataset, RelationalGroupedDataset}

/**
 * @Author: chen
 * @Date: 2023/7/5
 * @Desc:
 */
class GroupByTransform(groups: List[String], aggs: List[AggElement])
  extends AggregateTransform(groups, aggs) {

  override protected def getRGDataset[T](
      original: Dataset[T],
      groupColumn: List[Column]): RelationalGroupedDataset = {
    original.groupBy(groupColumn: _*)
  }
}
