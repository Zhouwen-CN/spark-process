package com.yeeiee.utils

import com.yeeiee.constants.CharsetConstant

import scala.io.{BufferedSource, Source}

object FileUtil {
  def read(path: String, encode: String = CharsetConstant.DEFAULT): String = {
    var file: BufferedSource = null
    try {
      file = Source.fromFile(path, encode)
      file.mkString
    } catch {
      case e: Exception => throw new Exception(s"cannot read file path [${path}]")
    } finally {
      if (Option(file).isDefined) {
        file.close()
      }
    }
  }

}
