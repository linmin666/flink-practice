package com.linmin.state

import org.apache.flink.api.common.state.{StateTtlConfig, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time

/**
  * @author Min.Lin
  * @since 2020-01-06 10:16
  *
  * 对任意Keyed State类型都可以设定State的生命周期(TTL)，
  * 以确保在规定的时间内及时清理状态数据。
  *
  * StateTtlConfig.UpdateType.OnCreateAndWrite：仅在创建和写入时更新TTL；
  * StateTtlConfig.UpdateType.OnReadAndWrite：所有读与写操作都更新TTL；
  * StateTtlConfig.StateVisibility.NeverReturnExpired：状态数据过期就不会返回（默认）；
  * StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp：状态数据即使过期但没有被清理依然返回；
  */
object StateTtlConfigDemo {
  def main(args: Array[String]): Unit = {
    //状态生命周期的配置
     val stateTtlConfig = StateTtlConfig
      //指定TTL时长为10s
      .newBuilder(Time.seconds(10))
      //指定TTL刷新时只对创建和写入操作有效；
      .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
      //指定状态可见性为永远不返回过期数据(默认)
      .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
      .build()
    //创建ValueStateDescriptor
    val valueStateDescriptor = new ValueStateDescriptor[Long]("valueState",classOf[Long])
    //指定创建好的stateTtlConfig
    valueStateDescriptor.enableTimeToLive(stateTtlConfig)
  }
}