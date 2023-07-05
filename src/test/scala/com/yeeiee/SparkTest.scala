package com.yeeiee

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

    import session.implicits._
    val df: DataFrame = session.sparkContext.makeRDD(Seq(
      (5, "zs"),
      (6, "ls"),
      (7, "ww")
    )).toDF("id", "name")


    session.sql("select * from " + df).show()*/

    //    println(ListBuffer(List("zs", "ls", "ww").indices.toList: _*))

    println(Option(List(0)).getOrElse(List.empty[String]).isEmpty)
  }
}
