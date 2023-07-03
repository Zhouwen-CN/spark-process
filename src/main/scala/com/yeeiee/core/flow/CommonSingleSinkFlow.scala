package com.yeeiee.core.flow

import com.yeeiee.core.env.ContextManager
import com.yeeiee.core.factory.{SinkFactory, SourceFactory, TransformFactory}
import com.yeeiee.core.params.ParamManager


class CommonSingleSinkFlow(config: Any, sources: List[Any], transforms: List[Any], sink: Any) extends AbstractFlow(config) {
  override def run(param: ParamManager): Unit = {
    val context: ContextManager = createContext(param)
    var tableNames: List[String] = sources.map(s => SourceFactory.build(s).run(context))

    Option(transforms) match {
      case None => logWarning("common single sink flow transforms is empty ...")
      case _ => transforms.foreach {
        transform =>
          tableNames = TransformFactory.build(transform).run(context, tableNames)
      }
    }

    SinkFactory.build(sink).run(context, tableNames.head)
  }
}
