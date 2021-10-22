package com.hjj.scala.datastream.sourcetest

import org.apache.flink.streaming.api.scala._

/**
 * @author huangJunJie 2021-10-21-19:49
 */
object UseCustomSource {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(2)

    //使用自定义source(无界流)
    val inputStream = streamEnv.addSource(new MyCustomSource)
    inputStream.print()

    //启动
    streamEnv.execute("custom source test")
  }

}
