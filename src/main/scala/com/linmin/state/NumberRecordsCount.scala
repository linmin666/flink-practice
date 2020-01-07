package com.linmin.state

import java.util
import java.util.Collections
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._
/**
  * @author Min.Lin
  * @since 2020-01-06 14:44
  *
  * 通过实现ListCheckpointed接口并利用Operator State计算算子输入数据量
  * ListCheckpointed接口只支持List的状态数据
  */
class NumberRecordsCount extends FlatMapFunction[(String,Long),(String,Long)]
 with ListCheckpointed[Long] {
  private var numberRecordsCount:Long=0L

  override def flatMap(value: (String, Long), out: Collector[(String, Long)]): Unit = {
    numberRecordsCount += 1
    out.collect((value._1,numberRecordsCount))
  }

  override def snapshotState(checkpointId: Long, ts: Long): util.List[Long] = {
    //snapshotState时将numberRecordCount写入
    Collections.singletonList(numberRecordsCount)
  }

  override def restoreState(list: util.List[Long]): Unit = {
    numberRecordsCount = 0L
    for(count <- list){
      //从状态中恢复numberRecordsCount
      numberRecordsCount=numberRecordsCount+count
      numberRecordsCount
    }
  }
}