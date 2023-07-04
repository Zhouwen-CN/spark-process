package com.yeeiee

import org.apache.spark.sql.SparkSession

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
object SparkTest {
  def main(args: Array[String]): Unit = {
    /*System.setProperty("HADOOP_USER_NAME", "atguigu")
    val session: SparkSession = SparkSession.builder().master("local[1]").appName("test")
      .enableHiveSupport()
      .getOrCreate()

    session.sql("select `id`, `name` from `student`").show()*/

    List(1) match {
      case List(x,_*) => println(111)
      case _ => println(2222)
    }
  }
}
