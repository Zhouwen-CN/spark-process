这里放一些json配置文件
也可以有很多子文件夹,比如按照业务模块划分

config: 定义一些配置
sources: 定义数据源的集合
sink: 定义写出的表
transforms: 定义一些转换

{
  "clazz": "CommonSingleSinkFlow",
  "config": {
    "spark.dynamicAllocation.maxExecutors": "16"
  },
  "sources": [
    {
      "out": "a",
      "clazz": "HiveSource",
      "table": "default.student"
    }
  ],
  "sink": {
    "in": "a",
    "clazz": "HiveSink",
    "mode": "overwrite",
    "table": "default.student1"
  },
  "transforms": [
  ]
}