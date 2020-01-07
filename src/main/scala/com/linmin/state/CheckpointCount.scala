package com.linmin.state

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._

/**
  * @author Min.Lin
  * @since 2020-01-06 11:10
  *
  * 在Flink中可以实现CheckpointedFunction或者ListCheckpointed<T extends Serializeable>
  * 两个接口来定义操作Managed Operator State
  * 通过CheckpointedFunction接口操作Operator State
  * 本类是通过CheckpointedFunction接口利用Operator State统计输入到算子的数据量
  */
private class CheckpointCount(val numElements:Int)
  extends FlatMapFunction[(Int,Long),(Int,Long,Long)]
  with CheckpointedFunction{
  //定义算子实例本地变量，存储Operator数据变量
  private var operatorCount:Long=_
  //定义keyedState，存储和key相关的状态值
  private var keyedState:ValueState[Long]=_
  //定义operatorState，存储算子状态值
  private var operatorState:ListState[Long]=_

  override def flatMap(value: (Int, Long), out: Collector[(Int, Long, Long)]): Unit = {
    val keyedCount=keyedState.value()+1
    keyedState.update(keyedCount)
    //更新**本地**算子operatorCount值
    operatorCount=operatorCount+1
    //(id,id对应的数量，本地算子输入数据的数量)
    out.collect((value._1,keyedCount,operatorCount))
  }

  //每次checkpoint触发时调用此方法
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
  //当发送snapshot时，把operatorcount添加到operatorState中
    operatorState.clear()
    operatorState.add(operatorCount)
  }

  //每次自定义函数初始化时，调用此方法初始化状态
  override def initializeState(context: FunctionInitializationContext): Unit = {
    keyedState=context.getKeyedStateStore.getState(
      new ValueStateDescriptor[Long]("keyedState",classOf[Long]))
    operatorState=context.getOperatorStateStore.getListState(
      new ListStateDescriptor[Long]("operatorState",classOf[Long]))

    //定义在Restored过程中，从operatorState中恢复数据的逻辑
    if(context.isRestored){
      operatorState.get().asScala.sum
    }
  }
}