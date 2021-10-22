package com.hjj.scala.datastream.sinktest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink

/**
 * @author huangJunJie 2021-10-22-14:16
 */
object FileSink {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val filePath="E:\\IDEA\\Projects\\FlinkProgram\\src\\main\\resources\\inputdata.txt"
    val inputStream: DataStream[String] = streamEnv.readTextFile(filePath)

    //输出到本地文件
    inputStream.writeAsText("E:\\IDEA\\Projects\\FlinkProgram\\src\\main\\resources\\outdata.txt").setParallelism(1)

    streamEnv.execute("file sink test")

  }

}
