package com.hjj.scala.tableApi_and_sql

import java.util.Properties

import com.hjj.scala.pojo.TemperatureSensor
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

/**
 * @author huangJunJie 2021-12-30-17:10
 *
 *         DataStream 转成 Table
 *
 *         对于一个DataSteam，可以直接转换成Table，进而方便地调用Table API做转换操作。且默认转换后的Table的Schema和DataStream
 *         中的字段定义一一对应，也可以单独指定出来。比如DataStream中数据是(A:String,B:String,C:String)这样的样例类实例，那么
 *         这个DataStream转换得来的Table的Scheme就是 A,B,C这3个字段
 */
object TableApiExample4 {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","192.168.204.101:9092")
//    properties.setProperty("group.id","group_1")

    val inputStream: DataStream[String] = streamEnv.addSource(new FlinkKafkaConsumer[String]("sensor",new SimpleStringSchema(),properties))
    val resultStream: DataStream[TemperatureSensor] = inputStream.map(data => {
      val fields = data.split(",")
      TemperatureSensor(fields(0).split(":")(1), fields(1).split(":")(1).toLong, fields(2).split(":")(1).toDouble)
    })

    //创建Table API环境
    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val streamTableEnv = StreamTableEnvironment.create(streamEnv, settings)
    //流转成表
    val inputTable = streamTableEnv.fromDataStream(resultStream)
    //对表进行transform
    val resultTabel = inputTable.select('id, 'temperature)

    //打印输出表（需要将表转成流才能打印输出）
    resultTabel.toAppendStream[(String,Double)].print()

    streamEnv.execute("Table API Test")
  }

}

