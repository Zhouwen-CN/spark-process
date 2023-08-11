package com.yeeiee.core.transform

import com.yeeiee.constants.NumberConstant
import com.yeeiee.core.env.ContextManager
import com.yeeiee.core.transform.abs.SingleOperandTransform
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.expr

/**
 * @Author: chen
 * @Date: 2023/7/5
 * @Desc:
 */
class RepartitionTransform(number: Int, shuffle: Boolean, range: Boolean, exprs: List[String])
  extends SingleOperandTransform {

  override protected def realRun(context: ContextManager, operands: List[DataFrame]): DataFrame = {
    val operand: DataFrame = operands(NumberConstant.NUMBER_0)

    val partition: Int = getPartition(operand)

    if (getShuffle) {
      deRepartition(operand, partition)
    } else {
      doCoalesce(operand, partition)
    }
  }

  private def doCoalesce(df: DataFrame, partition: Int): DataFrame = df.coalesce(partition)

  private def deRepartition(df: DataFrame, partition: Int): DataFrame = {
    val columns: List[Column] = getColumns

    if (getRange) {

      /**
       * 按照给定column的范围划分,比如col范围在1~10000,partition=4
       * 那么: 0~2500,2500~5000,5000~7500,7500~10000
       * 使用range重分区时,必须指定排序列
       */
      if (columns.isEmpty) {
        throw new Exception(
          "the repartition transform usage repartition by range you should configure columns ...")
      } else {
        df.repartitionByRange(partition, columns: _*)
      }
    } else {

      /**
       * 相当于hive sql: distribute by column
       * 或者 distribute by range
       */
      if (columns.isEmpty) {
        df.repartition(partition)
      } else {
        df.repartition(partition, columns: _*)
      }
    }
  }

  private def getPartition(df: DataFrame): Int = {
    val realNum: Int = Option(number).getOrElse(NumberConstant.NUMBER_0)
    if (NumberConstant.NUMBER_0 == realNum) {
      throw new Exception("the repartition transform you should configure partition number ...")
    }

    realNum
  }

  private def getShuffle: Boolean = Option(shuffle).getOrElse(NumberConstant.FALSE)

  private def getRange: Boolean = Option(range).getOrElse(NumberConstant.FALSE)

  private def getColumns: List[Column] = {
    Option(exprs).getOrElse(List.empty[String]).map(item => expr(item))
  }
}
