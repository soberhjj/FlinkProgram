package com.hjj.offcialdemo

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.{Alert, Transaction}

/**
 * @author huangJunJie 2021-11-16-14:39
 *
 */
object FraudDetector {
  val SMALL_AMOUNT: Double = 1.00
  val LARGE_AMOUNT: Double = 500.00
  val ONE_MINUTE: Long = 60 * 1000L
}


@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

  @transient private var flagState: ValueState[java.lang.Boolean] = _
  @transient private var timerState: ValueState[java.lang.Long] = _

  //状态在使用之前需要先被注册。 状态需要使用 KeyedProcessFunction#open() 函数来注册状态。
  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    //注册布尔类型的ValueState
    val flagDescriptor = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDescriptor)

    //注册Long类型的ValueState
    val timerDescriptor = new ValueStateDescriptor("timer-state", Types.LONG)
    timerState = getRuntimeContext.getState(timerDescriptor)
  }

  override def processElement(value: Transaction, ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context, out: Collector[Alert]): Unit = {
    // 获取当前key的state
    val keyCurrentState = flagState.value

    // Check if the key state is set
    if (keyCurrentState != null) {
      //如果出现小于 $1 美元的交易后紧跟着一个大于 $500 的交易，就输出一个报警信息。
      if (value.getAmount > FraudDetector.LARGE_AMOUNT) {
        val alert = new Alert
        alert.setId(value.getAccountId)

        out.collect(alert)
      }
      //清除状态(flagState和timerState这两个状态都清除掉，因为都失效了)
      cleanUp(ctx)
    }

    //判断是否需要重设状态（当本次交易额是小于 $1 美元时需重设状态）
    if (value.getAmount < FraudDetector.SMALL_AMOUNT){
      //设置 是否"交易是小于$1美元"的这个状态
      flagState.update(true)

      //设置定时器 和 定时器状态(即这里的timerState,timerState状态中保存了定时器的触发时间)
      //设定定时器有效期为1分钟
      val timer = ctx.timerService.currentProcessingTime + FraudDetector.ONE_MINUTE
      ctx.timerService.registerProcessingTimeTimer(timer)
      timerState.update(timer)

    }
  }

  //当定时器触发时，将会调用 KeyedProcessFunction#onTimer 方法。 通过重写这个方法来实现一个你自己的重置状态的回调逻辑。
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext, out: Collector[Alert]): Unit = {
    //定时器被触发，说明时间已经到达了1分钟，flagState保存的状态失效，所以flagState保存的状态应该被清除，同时定时器状态也清除
    flagState.clear()
    timerState.clear()
  }

  @throws[Exception]
  private def cleanUp(ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context): Unit = {
    // delete timer(删除定时器)
    val timer = timerState.value
    ctx.timerService.deleteProcessingTimeTimer(timer)

    // clean up all states(删除定时器状态 和 是否交易是小于$1美元的这个状态)
    timerState.clear()
    flagState.clear()
  }
}
