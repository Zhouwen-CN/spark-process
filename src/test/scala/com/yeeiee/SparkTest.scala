package com.yeeiee

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

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

    val list1: List[Int] = List(1, 2, 3, 4, 5, 6)
    val list2: List[Int] = List()

    println(list1.intersect(list2) == list2)
  }
}
