package com.hjj.scala.datastream.statetest

import com.hjj.scala.pojo.TemperatureSensor
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._

/**
 * @author huangJunJie 2021-10-22-16:09
 *
 *         需求：当温控传感器传入的两个连续温度值一旦相差>=10，就报警
 *
 *         思路：实现这个需求立马想到的是用reduce去实现，但是reduce这种方法有局限，因为reduce输出的数据类型要和输入的数据类型一样。
 *         这里用状态编程（使用map调用自定义的带状态的函数）来实现，当两个连续温度值一旦相差>=10，要输出报警信息
 *
 *         遗留问题：1、如何进行状态的初始化
 *                  2、RichMapFunction能否做到当温差>= threshold时，返回(in.id, lastTemp, in.temperature)，否则就不返回任何值呢？
 *                     已经知道RichFlatMapFunction是可以做到的，RichMapFunction能够做到呢？
 *
 *
 */
object StateTest1 {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(2)

    val inputStream = streamEnv.socketTextStream("localhost", 9999)

    val dataStream = inputStream.map(data => {
      val fields = data.split(",")
      TemperatureSensor(fields(0), fields(1).toLong, fields(2).toDouble)
    })

    val alterDataStream = dataStream.keyBy(_.id).map(new TempChangeAlert(10))

    alterDataStream.print()

    streamEnv.execute("state test")
  }

}

class TempChangeAlert(threshold: Double) extends RichMapFunction[TemperatureSensor, Any] {

  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valuestate", classOf[Double]))

  override def map(in: TemperatureSensor):Any = {
    //获取上次的温度值
    val lastTemp = lastTempState.value()
    //跟最新的温度值求差值作比较
    val diff = (in.temperature - lastTemp).abs
    //更新状态
    lastTempState.update(in.temperature)

    //当温差>= threshold时，输出(in.id, lastTemp, in.temperature)，否则输出None
    //问题：能否做到当温差>= threshold时，返回(in.id, lastTemp, in.temperature)，否则就不返回任何值呢？
    if (diff >= threshold) {
      (in.id, lastTemp, in.temperature)
    }else{
      None
    }
  }
}

