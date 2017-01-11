package com.nxcjh.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * 求最大最小值
  *
  * 数据准备
  * file1
  * 102
  * 10
  * 39
  * 109
  * 200
  * 11
  * 3
  * 90
  * 28
  *
  *
  * file2:
  * 5
  * 2
  * 30
  * 838
  * 10005
  *
  *
  * 预测结果:
  * max:	10005
  * min:	2
  *
  * Created by aimei02132 on 17/1/11.
  */
object DataMaxMin {

  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("DataMaxMin").setMaster("local")
    val sc = new SparkContext(conf)

    val contests = sc.textFile("/spark_input/five")
    contests.filter(_.trim.length>0)
      .map(line => ("key",line.trim.toInt))
      .groupByKey()
      .map(x => {
        var min = Integer.MAX_VALUE
        var max = Integer.MIN_VALUE

        for (num <- x._2){
          if(num > max){
            max = num
          }
          if (num < min){
            min = num
          }
        }
        (max,min)
      }).collect
      .foreach(x => {
        println("max:\t" + x._1)
        println("min:\t" + x._2)
      })

    //思路与MR类似, 先设定一个key, value为需要求最大与最小的集合, 然后在groupByKey聚合在一起处理.

  }
}
