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
    //创建执行环境
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
