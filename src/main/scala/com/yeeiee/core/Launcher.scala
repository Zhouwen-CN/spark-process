package com.yeeiee.core

import com.yeeiee.beans.Logging
import com.yeeiee.constants.StringConstant
import com.yeeiee.core.factory.FlowFactory
import com.yeeiee.core.flow.Flow
import com.yeeiee.core.params.{KeyValueParamManager, ParamManager}
import com.yeeiee.utils.FileUtil


/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
object Launcher extends Logging {
  def main(args: Array[String]): Unit = {
    /**
     * 模拟参数,脚本接受任务名称和分区日期,解析并封装参数
     * 1.配置文件名称 = 任务名称.json
     * 2.参数名1=参数值1
     * 3.参数名2=参数值2
     */
    // todo 临时修改
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val params: List[String] = List("D:\\work\\idea\\spark-process\\center\\config\\table_insert-default-student1.json", "task_name=table_insert-default-student1", "task_time=20230702")
    val taskConfigPath: String = params.head
    logInfo(s"loading task config path: [ $taskConfigPath ]")
    val realParams: List[String] = params.tail
    logInfo(s"loading parameters: [ ${realParams.mkString(",")} ]")

    var flowConfig: String = FileUtil.read(taskConfigPath)

    val paramManager: ParamManager = new KeyValueParamManager()

    paramManager.parse(realParams)
      .foreach(kv => {
        val wrapKey: String = paramManager.wrapParamKey(kv._1)
        flowConfig = flowConfig.replace(wrapKey, kv._2)
      })

    logInfo(s"building task for config: ${StringConstant.LINE_SEPARATOR}$flowConfig")

    val flow: Flow = FlowFactory.build(flowConfig)

    logInfo(s"running flow: ${flow.getClass}")
    flow.run(paramManager)
  }
}
