package com.hjj.scala.datastream

import org.apache.flink.streaming.api.scala._

/**
 * @author huangJunJie 2021-10-21-14:56
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //创建流处理的执行环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(2)

    //接收socket流
    val host = args(0)
    val port = args(1).toInt
    val inputDataStream = streamEnv.socketTextStream(host, port)

    //处理
    val resultDataStream = inputDataStream.flatMap(_.split(" ")).map((_, 1)).keyBy(_._1).sum(1)

    //打印结果
    resultDataStream.print().setParallelism(1)

    //启动任务执行
    streamEnv.execute("stream word count")
  }
}
