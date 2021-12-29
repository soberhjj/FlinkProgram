package com.hjj.scala.tableApi_and_sql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.{BatchTableEnvironment, StreamTableEnvironment}

/**
 * @author huangJunJie 2021-12-28-20:24
 *
 *         Table API环境的创建方式
 */
object TableApiExample1 {
  def main(args: Array[String]): Unit = {
//    /**
//     *  创建Table API环境
//     * 有两种planner,一个是Flink自带的planner(也叫old planner),另一个是Blink planner
//     */
//    //1.1(基于old planner的流处理)
//    //1.1.1创建流环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    //1.1.2创建Table API环境
//    val oldPlannerStreamTableEnv1 = StreamTableEnvironment.create(env)
//
//    //1.2(基于old planner的流处理)（实际2.1等同于2.2  它是2.2的简写形式）
//    val oldPlannerStreamSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
//    val oldPlannerStreamTableEnv2 = StreamTableEnvironment.create(env, oldPlannerStreamSettings)
//
//    //1.3(基于old planner的批处理)
//    //1.3.1创建批环境
//    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
//    //1.3.2创建Table API环境
//    val oldPlannerBatchTableEnv = BatchTableEnvironment.create(batchEnv)
//
//
//    //1.4基于blink planner的流处理
//    val blinkPlannerStreamSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
//    val blinkPlannerStreamTableEnv = StreamTableEnvironment.create(env, blinkPlannerStreamSettings)
//
//    //1.5基于blink planner的批处理
//    val blinkPlannerBatchSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
//    val blinkPlannerBatchTableEnv = TableEnvironment.create(blinkPlannerBatchSettings)
  }
}
