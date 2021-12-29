package com.hjj.scala.tableApi_and_sql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}



/**
 * @author huangJunJie 2021-12-29-14:30
 *
 *         创建Table: Table一般用来描述外部数据，比如文件、数据库表或消息队列的数据，也可以直接从DataStream转换而来
 */
object TableApiExample2 {
  def main(args: Array[String]): Unit = {
    //1.创建Table API环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    //2.连接外部系统(这里以文件为例)，读取数据，注册表
    val filePath = "E:\\IDEA\\Projects\\FlinkProgram\\src\\main\\resources\\inputdata.txt"
    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new OldCsv()) //声明数据格式化方式
      .withSchema(new Schema() //声明表的schema
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.STRING())
        .field("temperature", DataTypes.STRING()))
      .createTemporaryTable("inputTable")

    val inputTable: Table = tableEnv.from("inputTable")

    //打印输出table （注意：如果要打印输出table，需要将table转为一个流）
    inputTable.toAppendStream[(String,String,String)].print()

    env.execute("Table API Test")
  }
}
