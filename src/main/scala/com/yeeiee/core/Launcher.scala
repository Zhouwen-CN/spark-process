package com.yeeiee.core

import com.yeeiee.beans.Logging
import com.yeeiee.constants.StringConstant
import com.yeeiee.core.factory.FlowFactory
import com.yeeiee.core.flow.Flow
import com.yeeiee.core.params.{KeyValueParamManager, ParamManager}
import com.yeeiee.utils.FileUtil


/**
 * @Author: chen
 * @Date: 2023/7/2 14:21
 * @Desc:
 */
object Launcher extends Logging {
  def main(args: Array[String]): Unit = {
    /**
     * 模拟参数,由脚本传进来
     * 1.任务名称,配置文件名称 = 任务名称.json
     * 2.日期分区
     * 3.以及一些其他的自定义参数
     * tip: 任务调度只穿任务名,脚本find文件,并封装task_name参数
     */
    val params: List[String] = List("D:/work/study/spark-process/center/config/example.json", "task_name=example", "task_time=20230702")
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

    val flow: Flow = FlowFactory.build(
      flowConfig
    )

    logInfo(s"running flow: ${flow.getClass}")
    flow.run(paramManager)
  }
}
