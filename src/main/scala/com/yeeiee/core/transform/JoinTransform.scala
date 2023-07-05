package com.yeeiee.core.transform


import com.yeeiee.constants.{NumberConstant, StringConstant}
import com.yeeiee.core.env.ContextManager
import com.yeeiee.core.transform.JoinTransform._
import com.yeeiee.core.transform.abs.DoubleOperandTransform
import org.apache.spark.sql.functions.{broadcast, expr}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

import scala.collection.mutable.ListBuffer

/**
 * @Author: chen
 * @Date: 2023/7/5
 * @Desc:
 */
object JoinTransform {
  /**
   * join类型
   */
  private object JoinFunc {
    val INNER: String = "inner"
    val FULL: String = "full"
    val LEFT: String = "left"
    val RIGHT: String = "right"
    val LEFT_SEMI: String = "leftsemi"

    val DEFAULT: String = LEFT
  }

  /**
   * 冲突字段改名前缀
   */
  private object RenamePrefix {
    val LEFT: String = "_l_"
    val RIGHT: String = "_r_"
  }

  private object TableAlias {
    val LEFT: String = "l"
    val RIGHT: String = "r"
  }

  /**
   * 冲突字段以哪张表为主
   */
  private object ConflictStrategy {
    val LEFT: String = "l"
    val RIGHT: String = "r"
    val DEFAULT: String = StringConstant.EMPTY
  }

  /**
   * 广播哪张表
   */
  private object BroadcastStrategy {
    val LEFT: String = "l"
    val RIGHT: String = "r"

    val DEFAULT: String = StringConstant.EMPTY
  }
}

class JoinTransform(
                     func: String,
                     cs: String,
                     rpl: String,
                     rpr: String,
                     bs: String,
                     condition: String
                   ) extends DoubleOperandTransform {


  override protected def realRun(context: ContextManager, operands: List[DataFrame]): DataFrame = {
    val left: Dataset[Row] = operands(NumberConstant.NUMBER_0).alias(TableAlias.LEFT)
    val right: Dataset[Row] = operands(NumberConstant.NUMBER_1).alias(TableAlias.RIGHT)

    doJoin(left, right, getJoinType, getBroadcastStrategy, getCondition)
  }

  private def getJoinType: String = {
    Option(func).getOrElse(JoinFunc.DEFAULT) match {
      case JoinFunc.INNER => "inner"
      case JoinFunc.FULL => "outer"
      case JoinFunc.LEFT => "left_outer"
      case JoinFunc.RIGHT => "right_outer"
      case JoinFunc.LEFT_SEMI => "leftsemi"
      case _ =>
        throw new Exception(s"unknown join func: [ $func ] ...")
    }
  }

  private def getBroadcastStrategy: String = Option(bs).getOrElse(BroadcastStrategy.DEFAULT)

  private def getCondition: Column = {
    val onCondition: String = Option(condition).getOrElse(StringConstant.EMPTY)
    if (onCondition.isEmpty) {
      throw new Exception("the join transform condition must be not empty ...")
    }
    expr(condition)
  }

  private def doJoin(left: DataFrame, right: DataFrame, joinType: String, broadcastStrategy: String, onCondition: Column): DataFrame = {
    // 处理冲突字段
    val columns: List[Column] = resolveConflict(left, right)

    val joined: DataFrame = broadcastStrategy match {
      case BroadcastStrategy.DEFAULT =>
        left.join(right, onCondition, joinType)
      case BroadcastStrategy.LEFT =>
        broadcast(left).join(right, onCondition, joinType)
      case BroadcastStrategy.RIGHT =>
        left.join(broadcast(right), onCondition, joinType)
      case _ =>
        throw new Exception(s"unknown broadcast strategy: [ $broadcastStrategy ] ...")
    }

    joined.select(columns: _*)
  }

  private def resolveConflict(left: DataFrame, right: DataFrame): List[Column] = {
    // 冲突策略
    val conflicts: String = Option(cs).getOrElse(ConflictStrategy.DEFAULT)
    // 左表改名前缀
    val renameRpl: String = Option(rpl).getOrElse(RenamePrefix.LEFT)
    // 右表改名前缀
    val renameRpr: String = Option(rpr).getOrElse(RenamePrefix.RIGHT)
    // 左表列名
    val leftNames: List[String] = left.columns.toList
    // 右表列名
    val rightNames: List[String] = right.columns.toList
    // 冲突列表
    val intersect: List[String] = leftNames.intersect(rightNames)

    val result: ListBuffer[Column] = ListBuffer.empty[Column]

    leftNames.foreach {
      name =>
        appendResultColumn(result, ConflictStrategy.LEFT, conflicts, left, name, renameRpl, intersect)
    }
    rightNames.foreach {
      name =>
        appendResultColumn(result, ConflictStrategy.RIGHT, conflicts, right, name, renameRpr, intersect)
    }

    result.toList
  }

  private def appendResultColumn(result: ListBuffer[Column], current: String, strategy: String, df: DataFrame, name: String, prefix: String, intersect: List[String]): Unit = {
    val column: Column = df(name)
    if (intersect.contains(name)) {
      strategy match {
        case ConflictStrategy.LEFT | ConflictStrategy.RIGHT =>
          // 当前表是冲突策略主表,则添加
          if (current.equals(strategy)) {
            result.append(column)
          } else {
            logInfo(s"resolve conflict ignore right [ $name ] ...")
          }
        // 没有冲突策略,则给冲突字段添加前缀
        case _ =>
          result.append(column.alias(prefix + name))
      }
      // 不在冲突列表,直接添加
    } else {
      result.append(column)
    }
  }
}


