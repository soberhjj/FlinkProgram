package com.hjj.scala.datastream.transformtest

import com.hjj.scala.datastream.sourcetest.MyCustomSource
import org.apache.flink.streaming.api.scala._


/**
 * @author huangJunJie 2021-10-22-10:24
 */
object reducetest {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(2)

    //使用自定义source，模拟接收温度传感器数据
    val inputStream = streamEnv.addSource(new MyCustomSource)
    //使用reduce方式求每个传感器各自传入的最低温度
    val resultStream = inputStream
      .map(data => {
        val fields = data.split("\t")
        val sensor_id = fields(0).substring(10)
        val temperature = fields(1).substring(12)
        (sensor_id, temperature)
      })
      .keyBy(_._1)
      .reduce((x, y) => (x._1, math.min(x._2.toDouble, y._2.toDouble).toString))

    resultStream.print()

    streamEnv.execute("keyBy and reduce test")
  }

}
