package com.yeeiee.core.transform.abs

/**
 * @Author: chen
 * @Date: 2023/7/5
 * @Desc:
 */
abstract class MultipleOperandTransform extends AbstractTransform {

  override def getOsn: Int = 0

  override def getOssDefault: List[Int] = List.empty[Int]
}
