package com.hjj.scala.datastream.sourcetest

import org.apache.flink.streaming.api.scala._

/**
 * @author huangJunJie 2021-10-21-17:14
 */
object FileSource {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境（流处理执行环境是StreamExecutionEnvironment,批处理执行环境是ExecutionEnvironment）,(批处理环境用的是DataSet API,批处理环境用的是DataStream API)
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(2)

    //从文件读取数据 (有界流)
    val filePath="E:\\IDEA\\Projects\\FlinkProgram\\src\\main\\resources\\inputdata.txt"
    val inputStream: DataStream[String] = streamEnv.readTextFile(filePath)
    inputStream.print()


    //启动
    streamEnv.execute("file source test")
  }
}
