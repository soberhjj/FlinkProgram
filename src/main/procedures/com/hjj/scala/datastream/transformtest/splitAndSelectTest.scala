package com.hjj.scala.datastream.transformtest

import com.hjj.scala.datastream.sourcetest.MyCustomSource
import org.apache.flink.streaming.api.scala._

/**
 * @author huangJunJie 2021-10-22-11:15
 *
 *         利用split算子+select算子对流进行分流
 *
 *         split算子：DataStream -> SplitStream
 *         select算子：SplitStream -> DataStream  ，从一个SplitStream中获取一个或多个DataStream
 */
object splitAndSelectTest {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(2)

    //使用自定义source，模拟接收温度传感器数据
    val inputStream = streamEnv.addSource(new MyCustomSource)
    //分流：将传感器温度数据分成低温、高温两条流
    val splitStream = inputStream
      .map(data => {
        val fields = data.split("\t")
        val sensor_id = fields(0).substring(10)
        val temperature = fields(1).substring(12)
        (sensor_id, temperature)
      })
      .split(data => {
        if (data._2.toDouble > 20.0) Seq("high") else Seq("low")
      })

    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")

    highTempStream.print("high")
    lowTempStream.print("low")


    streamEnv.execute("split and select test")


  }

}
