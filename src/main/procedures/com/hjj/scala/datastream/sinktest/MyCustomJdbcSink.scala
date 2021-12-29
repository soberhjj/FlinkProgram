package com.hjj.scala.datastream.sinktest

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}


/**
 * @author huangJunJie 2021-10-22-14:39
 *
 *         注意 Flink从1.14版本开始已经提供JDBC Sink    依赖包：flink-connector-jdbc
 *
 */
object MyCustomJdbcSink {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    //比如输入数据的格式是"时间戳,uid,日期"
    val dataList = List("1639316838,873203,20211211","1639316844,656309,20211212","1639316856,237989,20211210")
    val sourceData: DataStream[String] = streamEnv.fromCollection(dataList)

    //然后截取 uid和日期 组成一个二元组
    val value = sourceData.map(data => {
      val fields = data.split(",")
      (fields(1), fields(2))
    })

    //输出
    value.addSink(new MyCustomJdbcSink())

    streamEnv.execute("test-custom-jdbc-sink")
  }

}

class MyCustomJdbcSink() extends RichSinkFunction[(String,String)] {

  var conn: Connection = _
  var insertStatement: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    //初始化连接
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/justtest", "root", "hjj19970829")
    //构造插入语句
    insertStatement = conn.prepareStatement("insert into a (uid,dt) values (?,?)")
  }

  override def invoke(value: (String,String)): Unit = {
    insertStatement.setString(1,value._1)
    insertStatement.setString(2,value._2)
    insertStatement.execute()
  }

  override def close(): Unit = {
    insertStatement.close()
    conn.close()
  }
}
