package com.hjj.scala.datastream.transformtest

import com.hjj.scala.datastream.sourcetest.MyCustomSource
import org.apache.flink.streaming.api.scala._

/**
 * @author huangJunJie 2021-10-21-20:12
 *
 *         keyBy：根据key对流进行分区  （DataStream -> KeyedStream）
 *
 *         对流进行分区后然后进行聚合操作，如sum()、min()、max()、minBy()、maxBy()等操作
 *
 *         所以keyBy本身不是目的，目的是keyBy之后进行聚合。
 */
object keyBytest {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(2)

    //使用自定义source，模拟接收温度传感器数据
    val inputStream = streamEnv.addSource(new MyCustomSource)
    //求每个传感器各自传入的最低温度
    val resultStream = inputStream
      .map(data => {
        val fields = data.split("\t")
        val sensor_id = fields(0).substring(10)
        val temperature = fields(1).substring(12)
        (sensor_id, temperature)
      })
      .keyBy(_._1)
      .min(1)

    resultStream.print()



    streamEnv.execute("keyBy and aggregation test")
  }
}
