package com.hjj.scala.datastream.sourcetest

import org.apache.flink.streaming.api.scala._

/**
 * @author huangJunJie 2021-10-21-19:49
 */
object UseCustomSource {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境（流处理执行环境是StreamExecutionEnvironment,批处理执行环境是ExecutionEnvironment）,(批处理环境用的是DataSet API,批处理环境用的是DataStream API)
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(2)

    //使用自定义source(无界流)
    val inputStream = streamEnv.addSource(new MyCustomSource)
    inputStream.print()

    //启动
    streamEnv.execute("custom source test")
  }

}
