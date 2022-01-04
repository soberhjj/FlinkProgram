package com.hjj.scala.datastream.sourcetest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @author huangJunJie 2021-10-21-17:22
 */
object KafkaSource {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境（流处理执行环境是StreamExecutionEnvironment,批处理执行环境是ExecutionEnvironment）,(批处理环境用的是DataSet API,批处理环境用的是DataStream API)
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(2)

    //从kafka读取数据 (无界流)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.204.101:9092")
    properties.setProperty("group.id","group_1")

    val inputStream: DataStream[String] = streamEnv.addSource(new FlinkKafkaConsumer[String]("flink-kafka-test",new SimpleStringSchema(),properties))

    inputStream.print()
    //启动
    streamEnv.execute("kafka source test")
  }
}
