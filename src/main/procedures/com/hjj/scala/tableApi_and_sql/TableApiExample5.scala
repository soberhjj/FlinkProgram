package com.hjj.scala.tableApi_and_sql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.descriptors._

/**
 * @author huangJunJie 2021-12-31-10:57
 *
 *         输出表：表的输出，是通过将数据写入TableSink来实现的。TableSink是一个通用接口，可以支持不同的文件格式、数据库和消息队列。
 *         输出表最直接的方法就是通过Table.insertInto()方法将一个Table写入注册过的TableSink中
 *
 *         如下将分别列举输出到本地文件、Kafka、Es  在TableApiExample5中将列举输出到Mysql
 */
object TableApiExample5 {
  def main(args: Array[String]): Unit = {
    //1.创建Table API环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    //2.连接外部系统，从kafka读取数据，注册表
    tableEnv.connect(new Kafka()
      .version("universal") //注意这里指定kafka版本：0.10 或 0.11 或 universal
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

    //3.对表进行transfrom(下面进行一个普通转换和一个聚合转换)
    //3.1普通转换
    val simpleTransformTable = kafkaInputTable.select('id, 'temperature)
    //3.2聚合转换
    val aggTable = kafkaInputTable
      .groupBy('id) //根据id进行分组
      .select('id, 'temperature.count() as 'cnt)

    /**
     * 注意对于聚合的Table，是不能通过Append方式输出的。
     * 比如若使用"aggTable.toAppendStream.print()"想把table输出到控制台，那么执行这行代码将会报错。应该Append输出方式只能用于
     * 那种在末尾进行数据追加的流或者Table，而聚合Table并不是简单的末尾进行数据追加，聚合Table涉及到对之前的数据进行update，
     * 所以如果想将聚合Table输出到控制台应该执行"aggTable.toRetractStream.print()"
     *
     * 其实这就是流式查询中的"更新模式"这个知识点：
     * 对于流式查询，需要声明如何在表和外部连接器进行数据交换，而数据交换的方式就是由"更新模式（Update Mode）"指定
     * 更新模式有三种：分别为追加（Append）模式、撤回（Retract）模式、更新插入（Upsert）模式
     * Append模式：只做追加（即在末尾插入）
     * Retract模式：可进行增删改
     * Upsert模式：可进行增删改，和Retract模式类似，但是二者的“表和外部连接器的数据交换方式”上是不同的。具体这里不展开
     */


    //4.输出表
    //4.1 输出到文件（这里用的是CsvTableSink,CsvTableSink仅支持Append模式）
    //4.1.1 注册输出表
    tableEnv.connect(new FileSystem().path("E:\\IDEA\\Projects\\FlinkProgram\\src\\main\\resources\\output_table_data"))
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.STRING()))
      .createTemporaryTable("fileOutputTable")
    //4.1.2 输出
    simpleTransformTable.insertInto("fileOutputTable")

    //4.2 输出到kafka（KafkaTableSink仅支持Append模式）
    //4.2.1 注册输出表
    tableEnv.connect(new Kafka()
      .version("universal")
      .topic("sensor_agg_result")
      .property("zookeeper.connnect", "192.168.204.101:2181")
      .property("bootstrap.servers", "192.168.204.101:9092"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.STRING()))
      .createTemporaryTable("kafkaOutputTable")
    //4.2.2 输出到输出表
    simpleTransformTable.insertInto("kafkaOutputTable")


    //注意这里有问题，上面写入文件和写入Kafka都没问题，这里写入ES导致启动报错。待后续排查解决该问题。
//    //4.3 输出到ES（这里用的是ElasticsearchUpsertTableSinkBase，Append模式/Retract模式/Upsert模式这三种模式ElasticsearchUpsertTableSinkBase都支持）
//    //4.3.1 注册输出表
//    tableEnv.connect(new Elasticsearch()
//      .version("7")
//      .host("192.168.204.103",9200,"http")
//      .index("sensor")
//      .documentType("temperature"))
//      .inUpsertMode()
//      .withFormat(new Json())
//      .withSchema(new Schema()
//        .field("id",DataTypes.STRING())
//        .field("cnt",DataTypes.BIGINT()))
//      .createTemporaryTable("ElasticsearchOutputTable")
//    //4.3.2 输出到输出表
//    simpleTransformTable.insertInto("ElasticsearchOutputTable")


    //注意这里是执行Table API的环境即tableEnv的execute方法。如果执行流环境即env的execute方法，则启动报错。
    //这是为什么？是因为这里的输入数据源是直接使用Table API创建（即tableEnv.connect...）的吗？该问题待后续排查解决。
    tableEnv.execute("Table API Test")
  }
}
