package com.hjj.scala.datastream.sourcetest

import com.hjj.scala.pojo.TemperatureSensor
import org.apache.flink.streaming.api.scala._

/**
 * @author huangJunJie 2021-10-21-16:58
 */
object CollectionSource {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(2)

    //从集合读取数据 (有界流)
    val dataList = List(TemperatureSensor("102110", 1634720599, 17.8),
      TemperatureSensor("102810", 1634706199, 22.8),
      TemperatureSensor("102117", 1634619799, 25.5))

    val inputStream = streamEnv.fromCollection(dataList)
    inputStream.print()

    //启动
    streamEnv.execute("collection source test")
  }
}
