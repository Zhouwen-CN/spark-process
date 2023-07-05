package com.yeeiee.core.flow

import com.yeeiee.core.env.ContextManager
import com.yeeiee.core.factory.{SinkFactory, SourceFactory, TransformFactory}
import com.yeeiee.core.params.ParamManager
import org.apache.spark.sql.DataFrame

/**
 * @Author: chen
 * @Date: 2023/7/2
 * @Desc:
 */
class CommonSingleSinkFlow(
                            config: Any,
                            sources: List[Any],
                            transforms: List[Any],
                            sink: Any
                          ) extends AbstractFlow(config) {
  override def run(param: ParamManager): Unit = {
    val context: ContextManager = createContext(param)
    var dfs: List[DataFrame] = sources.map(s => SourceFactory.build(s).run(context))

    // 当为null 或者 为空时, 打印日志
    transforms match {
      case List(x, _*) => transforms.foreach {
        transform =>
          dfs = TransformFactory.build(transform).run(context, dfs)
      }
      case _ => logWarning("common single sink flow transforms is empty ...")
    }

    SinkFactory.build(sink).run(context, dfs.head)
  }
}
