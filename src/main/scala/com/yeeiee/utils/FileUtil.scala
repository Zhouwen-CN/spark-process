package com.yeeiee.utils

import scala.io.{BufferedSource, Source}

import com.yeeiee.constants.CharsetConstant

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
object FileUtil {
  def read(path: String, encode: String = CharsetConstant.DEFAULT): String = {
    var file: BufferedSource = null
    try {
      file = Source.fromFile(path, encode)
      file.mkString
    } catch {
      case e: Exception => throw new Exception(s"cannot read file path [ $path ] ...")
    } finally {
      if (Option(file).isDefined) {
        file.close()
      }
    }
  }

}
