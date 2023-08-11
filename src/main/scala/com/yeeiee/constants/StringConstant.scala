package com.yeeiee.constants

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
object StringConstant extends Serializable {
  val EMPTY: String = ""
  val EQUAL: String = "="
  val POINT: String = "."
  val COMMA: String = ","
  val ASTERISK: String = "*"
  val SPACE: String = " "
  val STRIKETHROUGH: String = "-"
  val UNDERLINE: String = "_"
  val DOLLAR: String = "$"
  val POUND_KEY: String = "#"
  val LEFT_PARENTHESIS: String = "("
  val RIGHT_PARENTHESIS: String = ")"
  val LEFT_BRACKET: String = "["
  val RIGHT_BRACKET: String = "]"
  val LEFT_BRACE: String = "{"
  val RIGHT_BRACE: String = "}"
  val LINE_SEPARATOR: String = System.getProperty("line.separator")
  val TRUE: String = "true"
  val FALSE: String = "false"

  val TASK_NAME: String = "task_name"
  val TASK_TIME: String = "task_time"
}
