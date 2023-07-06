package com.yeeiee.core.env

import com.yeeiee.beans.Logging
import com.yeeiee.constants.StringConstant
import com.yeeiee.core.env.SessionProxy._
import com.yeeiee.core.params.ParamManager
import com.yeeiee.utils.SqlUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import java.util.UUID

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
object SessionProxy {
  /**
   * 自定义配置
   */
  private val PROXY_CONFIG: String = "proxy."
  private val TASK_PARALLELISM_MAX: String = "proxy.task.parallelism.max"

  /**
   * 基本参数
   */
  private val SPARK_YARN_QUEUE: String = "spark.yarn.queue"
  private val SPARK_DRIVER_CORES: String = "spark.driver.cores"
  private val SPARK_DRIVER_MEMORY: String = "spark.driver.memory"
  private val SPARK_EXECUTOR_MEMORY: String = "spark.executor.memory"
  private val SPARK_EXECUTOR_CORES: String = "spark.executor.cores"
  /**
   * 动态分配
   */
  private val SPARK_EXECUTORS_MAX: String = "spark.dynamicAllocation.maxExecutors"
  /**
   * executor number * executor core * 2
   */
  private val SPARK_SHUFFLE_PARTITIONS: String = "spark.sql.shuffle.partitions"
  /**
   * 自适应查询
   */
  private val SPARK_ADAPTIVE_ENABLED: String = "spark.sql.adaptive.enabled"
  /**
   * 广播join阈值, 20兆
   */
  private val SPARK_AUTO_BROADCAST_JOIN_THRESHOLD: String = "spark.sql.autoBroadcastJoinThreshold"
  /**
   * 广播超时
   */
  private val SPARK_BROADCAST_TIMEOUT: String = "spark.sql.broadcastTimeout"
  /**
   * hive参数
   */
  private val HIVE_DYNAMIC_PARTITION_ON: String = "hive.exec.dynamic.partition"
  private val HIVE_DYNAMIC_PARTITION_MODE: String = "hive.exec.dynamic.partition.mode"
  private val HIVE_DYNAMIC_PARTITION_MAX: String = "hive.exec.max.dynamic.partitions"
}

class SessionProxy(appName: String, userConfig: ConfigManager, jobParam: ParamManager) extends Logging {

  private val defaultConfig: Map[String, String] = Map[String, String](
    SPARK_YARN_QUEUE -> "default",
    SPARK_DRIVER_CORES -> "1",
    SPARK_DRIVER_MEMORY -> "512M",
    SPARK_EXECUTOR_MEMORY -> "1024M",
    SPARK_EXECUTOR_CORES -> "1",
    SPARK_EXECUTORS_MAX -> "4",
    SPARK_SHUFFLE_PARTITIONS -> "8",
    SPARK_ADAPTIVE_ENABLED -> "true",
    SPARK_AUTO_BROADCAST_JOIN_THRESHOLD -> "20971520",
    SPARK_BROADCAST_TIMEOUT -> "300",
    HIVE_DYNAMIC_PARTITION_ON -> "true",
    HIVE_DYNAMIC_PARTITION_MODE -> "nonstrict",
    HIVE_DYNAMIC_PARTITION_MAX -> "1000"
  )

  /**
   * 自定义函数
   */
  private val userDefineFunctionRegister: Map[String, UserDefinedFunction] = Map[String, UserDefinedFunction](

  )

  private val sparkSession: SparkSession = initialize()
  private val sparkConfig: Map[String, String] = configs
  private val sparkCatalog: Catalog = sparkSession.catalog
  private val sparkContext: SparkContext = sparkSession.sparkContext
  private val sqlContext: SQLContext = sparkSession.sqlContext

  private def initialize(): SparkSession = {
    logInfo("session proxy will build SparkSession ...")
    val builder: SparkSession.Builder = SparkSession.builder()

    logInfo(s"session proxy will set appName: $appName ...")
    builder.appName(appName)

    logInfo("session proxy will load defaultConfig ...")
    defaultConfig.foreach(item => config(builder, item))

    logInfo("session proxy will load userConfig ...")
    userConfig.foreach(item => config(builder, item))

    // todo 临时修改

    //    builder.master("local[2]")

    val session: SparkSession = builder.enableHiveSupport().getOrCreate()

    userDefineFunctionRegister.foreach(item => session.udf.register(item._1, item._2))

    session
  }

  private def config(builder: SparkSession.Builder, item: (String, String)): Unit = {
    val key: String = item._1
    val value: String = item._2
    if (key.startsWith(PROXY_CONFIG)) {
      logInfo(s"proxy loading proxy config: $key = $value")
      configProxy(builder, key, value)
    } else {
      logInfo(s"proxy loading direct config: $key = $value")
      configSession(builder, key, value)
    }
  }

  private def configSession(builder: SparkSession.Builder, key: String, value: String): Unit = {
    builder.config(key, value)
  }

  private def configProxy(builder: SparkSession.Builder, key: String, value: String): Unit = {
    key match {
      case TASK_PARALLELISM_MAX =>
        val parallelism: Int = value.toInt
        // shuffle = executor number * executor core * 2
        val executorMax: Int = parallelism
        val executorCores: Int = 2
        val suggestRatio: Int = 2
        val shufflePartitions: Int = executorMax * executorCores * suggestRatio

        builder.config(SPARK_EXECUTORS_MAX, executorMax)
        builder.config(SPARK_EXECUTOR_CORES, executorCores)
        builder.config(SPARK_SHUFFLE_PARTITIONS, shufflePartitions)

      case _ =>
        logWarning(s"not support proxy config: $key = $value")

    }
  }

  private def configs: Map[String, String] = {
    val configs: Map[String, String] = sparkSession.conf.getAll
    logInfo("proxy will show configs ...")
    for ((k, v) <- configs) {
      logInfo(s"proxy show config: $k = $v")
    }

    configs
  }

  private def executeSql(sqlText: String): DataFrame = {
    logInfo(s"proxy will running sql: $sqlText")
    sqlContext.sql(sqlText)
  }

  def createTable(exprs: List[String], table: String, filter: String): DataFrame = {
    val sqlText: String =
      s"""
         |${SqlUtil.select(exprs)}
         |${SqlUtil.from(table)}
         |${SqlUtil.where(filter)}
         |""".stripMargin
    executeSql(sqlText)
  }

  def insertTable(output: DataFrame, mode: String, out: String, partition: String): Unit = {
    val uuid: String = UUID.randomUUID().toString.replaceAll(StringConstant.STRIKETHROUGH, StringConstant.EMPTY)
    output.createOrReplaceTempView(uuid)
    val sqlText: String =
      s"""
         |${SqlUtil.insert(mode, out)}
         |${SqlUtil.partition(partition)}
         |${SqlUtil.select()}
         |${SqlUtil.from(uuid, check = false)}"""
        .stripMargin
    executeSql(sqlText)
    sparkCatalog.dropTempView(uuid)
  }

  def descTable(table: String): DataFrame = {
    val sqlText: String = SqlUtil.desc(table)
    executeSql(sqlText)
  }
}