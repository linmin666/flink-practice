package com.linmin.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * @author Min.Lin
  * @since 2020-01-05 11:46
  *
  * 获取指标最小值
  */
object KeyedStateManaged {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream: DataStream[(Int, Double)] = env.fromElements((2, 21.0), (4, 1.0), (5, 4.0))


    val result: DataStream[(Int, Double, Double)] = inputStream.keyBy(_._1).flatMap {
      //RichFlatMapFunction第一个参数是入参，第二个是出参
      new RichFlatMapFunction[(Int, Double), (Int, Double, Double)] {
        private var leastValueState: ValueState[Double] = _

        override def open(parameters: Configuration): Unit = {
          //创建ValueStateDescription，定义状态名为leastValue，类型为Double
          val leastValueDescriptor = new ValueStateDescriptor[Double]("leastValue", classOf[Double])
//          leastValueDescriptor.enableTimeToLive(stateTtlConfig)
          //getRuntimeContext.getState获取State
          leastValueState = getRuntimeContext.getState(leastValueDescriptor)

        }
        override def flatMap(t: (Int, Double), out: Collector[(Int, Double, Double)]): Unit = {
          //通过value方法从leastValueState中获取当前最小值
          val leastValue: Double = leastValueState.value()
          if (t._2 > leastValue) {
            out.collect((t._1, t._2, leastValue))
          }
          else {
            //当前指标小于leastValue，则更新状态中的最小值
            leastValueState.update(t._2)
            //将当前数据作为指标最小值输出
            out.collect((t._1, t._2, t._2))
          }

        }
      }
    }
    result.print()
    env.execute("State Test")

  }

}
