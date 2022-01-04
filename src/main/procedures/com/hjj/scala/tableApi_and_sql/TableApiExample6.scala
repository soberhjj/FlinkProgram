package com.hjj.scala.tableApi_and_sql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors._

/**
 * @author huangJunJie 2021-12-31-16:50
 *
 *         输出表：输出到Mysql
 *
 *         //启动运行报错(和输出到ES报同样的错误)，待后续排查问题
 */
object TableApiExample6 {
  def main(args: Array[String]): Unit = {
    //1.创建Table API环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    //2.连接读取本地文件并注册表
    tableEnv.connect(new FileSystem().path("E:\\IDEA\\Projects\\FlinkProgram\\src\\main\\resources\\inputdata.txt"))
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.STRING())
        .field("temperature", DataTypes.STRING()))
      .createTemporaryTable("fileInputTable")

    val fileInputTable: Table = tableEnv.from("fileInputTable")

    //3.转换
    val aggTable = fileInputTable
      .groupBy('id) //根据id进行分组
      .select('id, 'temperature.count() as 'cnt)

    /**
     *    4.输出表到mysql
     * 在之前输出到kafka或es时，用的都是 tableEnv.connect() 这种形式来创建Table API与外部系统的连接器，
     * 接下来建立Table API与Mysql的连接器时换一种写法。（如下）
     */
    //4.1连接Mysql,注册输出表
    val sinkDLL: String =
    """
      |create table jdbcOutputTable(
      | id varchar(20) not null,
      | cnt bigint not null
      |)with(
      | 'connector.type'='jdbc',
      | 'connector.url'='jdbc:mysql://localhost:3306/justtest?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true&useSSL=false',
      | 'connector.table'='sensor_count',
      | 'connector.driver'='com.mysql.cj.jdbc.Driver',
      | 'connector.username'='root',
      | 'connector.password'='hjj19970829'
      |)
      |""".stripMargin
    tableEnv.sqlUpdate(sinkDLL)

    //4.2输出
    aggTable.insertInto("jdbcOutputTable")

    tableEnv.execute("Table API Test")
  }
}
