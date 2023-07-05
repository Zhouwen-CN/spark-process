package com.yeeiee.beans

/**
 * @Author: chen
 * @Date: 2023/7/5
 * @Desc:
 */
abstract class TransformAttribute extends Attribute {

  private val oss: List[Int] = List.empty[Int]

  def getOss: List[Int] = oss

  def getOsn: Int

  def getOssDefault: List[Int]
}
