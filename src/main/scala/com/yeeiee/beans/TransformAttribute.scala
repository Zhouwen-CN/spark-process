package com.yeeiee.beans

/**
 * @Author: chen
 * @Date: 2023/7/5
 * @Desc:
 */
abstract class TransformAttribute extends Attribute {

  /**
   * 操作数列表
   */
  private val oss: List[Int] = List.empty[Int]

  def getOss: List[Int] = oss

  /**
   * 操作数个数
   *
   * @return
   */
  def getOsn: Int

  /**
   * 当没有给定操作数列表时,默认的操作数列表
   *
   * @return
   */
  def getOssDefault: List[Int]
}
