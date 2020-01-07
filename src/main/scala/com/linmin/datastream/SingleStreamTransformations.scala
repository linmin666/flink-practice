package com.linmin.datastream

import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
/**
  * @author Min.Lin
  * @since 2020-01-06 16:02
  *
  * 对单个DataStream数据集元素的处理逻辑
  */
object SingleStreamTransformations {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream: DataStream[(String, Int)] = env.fromElements(("a",3),("d",4),("c",2),("c",5),("a",5))

    //(1)Map[DataStream->DataStream]
    val mapResult = inputStream.map(t=>(t._1,t._2+1))

    //(2)FlatMap[DataStream->DataStream]
    val dataStream: DataStream[String] = env.fromCollection(Seq[String]("Flink is good for Streaming Processing !","Hello World"))
    val flatMapResult: DataStream[String] = dataStream.flatMap(_.split(" "))

    //(3)Fliter[DataStream->DataStream]
    val filterResult: DataStream[(String, Int)] = inputStream.filter(_._2 % 2 == 0)

    //KeyBy[DataStream->KeyedStream]
    val keyByResult: KeyedStream[(String, Int), String] = inputStream.keyBy(_._1)

    //Reduce[KeyedStream->DataStream]
    val reduceResult: DataStream[(String, Int)] = keyByResult.reduce((t1, t2)=>(t1._1,t1._2+t2._2))

    //Aggregations[KeyedStream->DataStream]
    //sum,min,minBy,max,maxBy
    //对第二个字段进行sum统计
    val sumResult: DataStream[(String, Int)] = keyByResult.sum(1)
    //滚动计算对应key的最小值,max同理
    val minResult: DataStream[(String, Int)] = keyByResult.min(1)
    //滚动计算对应key的最小值，返回最小值对应的元素
    val minByResult: DataStream[(String, Int)] = keyByResult.minBy(1)


//    mapResult.print()
//
//    flatMapResult.print()
//
//    filterResult.print()
//
    keyByResult.print()
//
//    reduceResult.print()
//
//    sumResult.print()
//
//    minResult.print()
//
//    minByResult.print()
    env.execute("Single DataStream Transformation")





  }
}