package com.yeeiee.core.transform

import com.yeeiee.core.transform.abs.AssembleTransform
import org.apache.spark.sql.DataFrame

/**
 * @Author: chen
 * @Date: 2023/7/5
 * @Desc:
 */
class UnionTransform(alignment: Boolean, distinct: Boolean)
  extends AssembleTransform(alignment, distinct) {

  override protected def assemble(head: DataFrame, other: DataFrame): DataFrame = {
    head.union(other)
  }
}
