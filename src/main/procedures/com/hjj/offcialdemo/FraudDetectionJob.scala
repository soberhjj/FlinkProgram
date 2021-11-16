package com.hjj.offcialdemo

import org.apache.flink.streaming.api.scala._
import org.apache.flink.walkthrough.common.sink.AlertSink
import org.apache.flink.walkthrough.common.source.TransactionSource

/**
 * @author huangJunJie 2021-11-16-15:19
 *
 *         官网案例：基于DataStream API实现欺诈检测
 *         https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/try-flink/datastream/
 *
 *         需求：对于一个账户，如果出现小于 $1 美元的交易后紧跟着一个大于 $500 的交易，就输出一个报警信息。
 */
object FraudDetectionJob {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //数据源
    val transactions = env.addSource(new TransactionSource).name("transactions").setParallelism(1)

    //处理逻辑
    val alters = transactions.keyBy(transaction => transaction.getAccountId).process(new FraudDetector).name("fraud-detector")

    //输出
    //addSink(new AlertSink)是输出到日志中，所以配置好日志输出(log4j.properties)后，再用该sink，然后去输入日志中看输出数据
//    alters.addSink(new AlertSink).name("send-alerts")
    alters.print()

    env.execute("Fraud Detection")
  }
}
