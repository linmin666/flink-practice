package com.linmin.state

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * @author Min.Lin
  * @since 2020-01-06 10:30
  *
  * 除了自定义RichFlatMapFunction或FichMapFunction操作状态以外，
  * 在keyedStream中提供了`filterWithState`,`mapWithState`,`flatMapWithState`
  * 三个方法来定义和操作状态数据；
  */
object DirectUseStateDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream: DataStream[(Int, Long)] = env.fromElements((997,2L),(4,1L),(5,4L),(997,7L))
    val result: DataStream[(Int, Long)] = inputStream
      .keyBy(_._1)
      //指定输入输出参数类型
      .mapWithState((in: (Int, Long), count: Option[Long]) => {
      //判断count是否为空
      count match {
        //输出key，count，并在原来的count数据上累加
        case Some(c) => ((in._1, c+in._2), Some(c + in._2))
        //若输入状态为空，则按指标输出
        case None => ((in._1, in._2), Some(in._2))
      }
    })
    result.print()
    env.execute("Direct Use State")

  }

}