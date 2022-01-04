package com.hjj.scala.tableApi_and_sql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, Schema}

/**
 * @author huangJunJie 2021-12-29-14:30
 *
 *      从kafka读取数据注册表，并进行查询转换
 *
 */
object TableApiExample3 {
  def main(args: Array[String]): Unit = {
    //1.创建Table API环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    //2.连接外部系统，从kafka读取数据，注册表
    tableEnv.connect(new Kafka()
      .version("universal")
      .topic("sensor")
      .property("zookeeper.connnect", "192.168.204.101:2181")
      .property("bootstrap.servers", "192.168.204.101:9092"))
      .withFormat(new Csv()) //声明数据格式化方式
      .withSchema(new Schema() //声明表的schema
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.STRING())
        .field("temperature", DataTypes.STRING()))
      .createTemporaryTable("kafkaInputTable")

    val kafkaInputTable: Table = tableEnv.from("kafkaInputTable")

    //3.查询转换
    //3.1使用Table API的方式（即DSL风格）
    val resultDslTable = kafkaInputTable.select('id, 'temperature).filter('id === "sensor_id:102110")

    //3.2使用SQL的方式
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id,temperature
        |from kafkaInputTable
        |where id='sensor_id:102110'
        |""".stripMargin)

    //打印输出table
    resultDslTable.toAppendStream[(String,String)].print("resultDslTable")
    resultSqlTable.toAppendStream[(String,String)].print("resultSqlTable")

    env.execute("Table API Test")
  }
}
