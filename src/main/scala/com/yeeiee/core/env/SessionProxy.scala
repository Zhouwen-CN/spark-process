package com.yeeiee.core.env

import com.yeeiee.beans.Logging
import com.yeeiee.core.env.SessionProxy._
import com.yeeiee.core.params.ParamManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{SQLContext, SparkSession}

object SessionProxy {
  private val PROXY_CONFIG: String = "proxy."
  private val TASK_PARALLELISM_MAX: String = "proxy.task.parallelism.max"

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
    SPARK_DRIVER_MEMORY -> "1024M",
    SPARK_EXECUTOR_MEMORY -> "2048M",
    SPARK_EXECUTOR_CORES -> "2",
    SPARK_EXECUTORS_MAX -> "4",
    SPARK_SHUFFLE_PARTITIONS -> "16",
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
    builder.master("local[2]")

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
        logWarning("not support proxy config: $key = $value")

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

  /*private def sql(sqlText: String): DataFrame = {
    logInfo(s"Proxy will run sql: ${sqlText}")
    sqlContext.sql(sqlText)
  }

  private def createTable(
                           df: DataFrame,
                           table: String,
                           partitioned: String = StringConstant.EMPTY,
                           stored: String,
                           location: String,
                           properties: Map[String, String]
                         ): Unit = {
    val view = createTempView(df)
    val sqlText =
      s"""
         |${GrammarUtil.createTable(table)}
         |${GrammarUtil.metaData(df)}
         |${GrammarUtil.partitionedBy(partitioned)}
         |${GrammarUtil.storedAs(stored)}
         |${GrammarUtil.locationAt(location)}
         |${GrammarUtil.tableProperties(properties)}
         |${GrammarUtil.asSelect()}
         |${GrammarUtil.from(view, check = false)}"""
        .stripMargin
    sql(sqlText)
    removeTempView(view)
  }

  def getConfig(key: String): Option[String] = {
    sparkConfig.get(key)
  }

  def broadcast[T: ClassTag](value: T): Broadcast[T] = sparkContext.broadcast(value)

  def binaryFiles(path: String): RDD[(String, PortableDataStream)] = {
    sparkContext.binaryFiles(path)
  }

  def binaryFiles(path: String, minPartitions: Int): RDD[(String, PortableDataStream)] = {
    sparkContext.binaryFiles(path, minPartitions)
  }

  def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame = {
    sqlContext.createDataFrame(rowRDD, schema)
  }

  def createRDD[T](seq: Seq[T]): RDD[T] = {
    sparkContext.makeRDD(seq)
  }


  def createDataFrame(
                       format: String,
                       options: scala.collection.Map[String, String]
                     ): DataFrame = {
    sparkSession.read.format(format).options(options).load()
  }

  def createDataFrame(
                       exprs: List[String],
                       table: String,
                       filter: String = StringConstant.EMPTY
                     ): DataFrame = {
    val sqlText =
      s"""
         |${GrammarUtil.select(exprs)}
         |${GrammarUtil.from(table)}
         |${GrammarUtil.where(filter)}"""
        .stripMargin
    sql(sqlText)
  }

  def createTempView(df: DataFrame, view: String = BaseUtil.generateUUID()): String = {
    df.createTempView(view)
    view
  }

  def createGlobalTempView(df: DataFrame, view: String = BaseUtil.generateUUID()): String = {
    df.createGlobalTempView(view)
    view
  }

  def removeTempView(view: String): Unit = {
    sparkCatalog.dropTempView(view)
  }

  def removeGlobalTempView(view: String): Unit = {
    sparkCatalog.dropGlobalTempView(view)
  }

  def registerUDAF(
                    name: String,
                    udaf: UserDefinedAggregateFunction
                  ): UserDefinedAggregateFunction = {
    sparkSession.udf.register(name, udaf)
  }

  def registerUDAF(
                    udaf: UserDefinedAggregateFunction
                  ): UserDefinedAggregateFunction = {
    val className = udaf.getClass.getSimpleName
    registerUDAF(className, udaf)
  }

  def createTable(
                   df: DataFrame,
                   table: String
                 ): Unit = {
    createTable(
      df,
      s"${table}_${jobParam.get(ParamConstant.TASK_TIME)}",
      StringConstant.EMPTY,
      FileFormat.ORC.toString,
      StringConstant.EMPTY,
      Map[String, String](
        (TableProperty.ORCCOMPRESS.toString, ORCCompress.SNAPPY.toString)
      )
    )
  }

  def insertTable(
                   df: DataFrame,
                   mode: String,
                   table: String,
                   partition: String
                 ): Unit = {
    val view = createTempView(df)
    val sqlText =
      s"""
         |${GrammarUtil.insert(mode, table)}
         |${GrammarUtil.partitionOf(partition)}
         |${GrammarUtil.select()}
         |${GrammarUtil.from(view, check = false)}"""
        .stripMargin
    sql(sqlText)
    removeTempView(view)
  }

  def descTable(table: String): DataFrame = {
    val sqlText = GrammarUtil.desc(table)
    sql(sqlText)
  }*/
}
