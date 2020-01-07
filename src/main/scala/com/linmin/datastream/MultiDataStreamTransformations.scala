package com.linmin.datastream

import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction}
import org.apache.flink.util.Collector

/**
  * @author Min.Lin
  * @since 2020-01-06 17:16
  *
  * 对多个DataStream数据集元素的处理逻辑
  */
object MultiDataStreamTransformations {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream1: DataStream[(String, Int)] = env.fromElements(("a", 3), ("d", 4), ("c", 2), ("c", 5), ("a", 5))
    val inputStream2: DataStream[(String, Int)] = env.fromElements(("z", 4), ("x", 9), ("y", 2), ("d", 5), ("q", 5))

    //(1)Union[DataStream->DataStream]
    val unionStream: DataStream[(String, Int)] = inputStream1.union(inputStream2)

    //(2)Connect,CoMap,CoFlatMap[DataStream->DataStream]
    //connect:合并后保留原数据集的数据类型，允许共享状态数据；
    val dataStream: DataStream[Int] = env.fromElements(1, 2, 4, 5, 6)
    val connectResult: ConnectedStreams[(String, Int), Int] = inputStream1.connect(dataStream)
    //connectResult无法直接print,需转为DataStream；可使用map或flatMap方法，传入自定义CoMap或CoFlatMap
    //2.1 CoMapFunction
    val connectMapResult: DataStream[(Int, String)] = connectResult
      .map(new CoMapFunction[(String, Int), Int, (Int, String)] {
        //map1与map2的返回类型必须一致
        //定义第一个数据集函数处理逻辑，输入值为第一个DataStream
        override def map1(in1: (String, Int)): (Int, String) = {
          (in1._2, in1._1)
        }

        //定义第二个数据集函数处理逻辑，输入值为第二个DataStream
        override def map2(in2: Int): (Int, String) = {
          (in2, "default")
        }
      })


    //2.2 CoFlatMapFunction
    val connectFlatMapResult: DataStream[(String, Int, Int)] = connectResult.flatMap(new CoFlatMapFunction[(String, Int), Int, (String, Int, Int)] {
      //定义共享变量
      var number = 0

      override def flatMap1(in1: (String, Int), collector: Collector[(String, Int, Int)]): Unit = {
        collector.collect((in1._1, in1._2, number))
      }

      override def flatMap2(in2: Int, collector: Collector[(String, Int, Int)]): Unit = {
        number = in2
      }
    })


    //(3)Split[DataStream->SplitStream]
    val splitResult = inputStream1.split(t => if (t._2 % 2 == 0) Seq("even") else Seq("odd"))

    //(4)Select[Split->DataStream]
    val evenStream = splitResult.select("even")
    val oddStream=splitResult.select("odd")
    //(5)Iterate[DataStream->IterativeStream->DataStream]
    val dataStream1: DataStream[Int] = env.fromElements(3, 1, 2, 1, 3,997).map { t: Int => t }

    val iterateResult: DataStream[String] = dataStream1.iterate((input: ConnectedStreams[Int, String]) => {
      //定义map方法完成对输入ConnectedStreams的处理
      val head: DataStream[String] = input.map(i => (i + 1).toString, s => s)
      (head.filter(_ == "2"), head.filter(_ != "2"))
    }, 1000) //超过1000ms没有数据接入则终止迭代

    env.execute("Multi-DataStream Transformations Demo")

  }
}