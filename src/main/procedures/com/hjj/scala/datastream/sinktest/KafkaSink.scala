package com.hjj.scala.datastream.sinktest

import org.apache.flink.streaming.api.scala._

/**
 * @author huangJunJie 2021-10-22-14:36
 */
object KafkaSink {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(2)

    //

    streamEnv.execute("kafka sink test")
  }

}
