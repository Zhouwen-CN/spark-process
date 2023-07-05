package com.yeeiee

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
object SparkTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val session: SparkSession = SparkSession.builder().master("local[1]").appName("test")
      .enableHiveSupport()
      .getOrCreate()

    import session.implicits._
    val df1: DataFrame = session.sparkContext.makeRDD(Seq(
      ("a", 1), ("a", 1), ("b", 3), ("c", 4)
    )).toDF("id", "name")

    val df2: DataFrame = session.sparkContext.makeRDD(Seq(
      ("a", 1), ("a", 1), ("b", 3)
    )).toDF("name", "id")


    df1.intersect(df2).show()

  }

  case class Student(id: Int, name: String)
}
