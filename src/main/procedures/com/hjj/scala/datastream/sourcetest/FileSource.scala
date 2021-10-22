package com.hjj.scala.datastream.sourcetest

import org.apache.flink.streaming.api.scala._

/**
 * @author huangJunJie 2021-10-21-17:14
 */
object FileSource {
  def main(args: Array[String]): Unit = {
    //创建执行环境
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
