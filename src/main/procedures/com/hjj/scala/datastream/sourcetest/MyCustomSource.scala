package com.hjj.scala.datastream.sourcetest

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.collection.immutable
import scala.util.Random

/**
 * @author huangJunJie 2021-10-21-17:39
 */
class MyCustomSource extends SourceFunction[String] {

  //定义一个标志位flag，用来表示数据源是否正常运行发出数据
  var runningFlag: Boolean = true

  //准备发出数据
  var temperatureData: immutable.IndexedSeq[(String, String)] = 1.to(10).map(i => ("sensor_id:" + i, "temperature:" + Random.nextDouble() * 100 % 40))

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (runningFlag) {
      //调用ctx.collect发出数据
      temperatureData.foreach(data=>ctx.collect(data._1+"\t"+data._2))

      //对数据进行调整，这样下次发出就是调整后的数据，模拟数据的变动
      temperatureData = temperatureData.map(data => (data._1, "temperature:" + Random.nextDouble() * 100 % 40))

      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = runningFlag = false
}
