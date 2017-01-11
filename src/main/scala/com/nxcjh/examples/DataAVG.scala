package com.nxcjh.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 平均成绩
  *
  * 1. 案例分析:
  * 需求分析:
  * 对输入文件中数据进行计算学生平均成绩. 输入文件中的每行内容均为一个学生的姓名和他相应的成绩, 如果有多门学科,
  * 则每门学科为一个文件. 要求在输出中每行有两个间隔的数据, 其中, 第一个代表学生的姓名, 第二个代表其平均成绩.
  *
  * 原始数据:
  * math
  * 张三    88
  * 李四    99
  * 王五    66
  * 赵六    77
  *
  *
  * china:
  * 张三    78
  * 李四    89
  * 王五    96
  * 赵六    67
  *
  *english:
  * 张三    80
  * 李四    82
  * 王五    84
  * 赵六    86
  *
  *
  * 思路: 先groupBy分组 , 在map处理成绩的集合, 并做了格式显示
  *
  *
  * 样本输出:
  *张三    82
  * 李四    90
  * 王五    82
  * 赵六    76
  *
  *
  *
  * Created by aimei02132 on 17/1/11.
  */
object DataAVG {


  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("DataAVG").setMaster("local")
    val sc = new SparkContext(conf)

    val contests = sc.textFile("/spark_input/four")
    contests.filter(_.trim.length>0)
      .map(x => (x.split("\t")(0).trim,x.split("\t")(1).trim.toInt))
        .groupByKey()
        .map(x => {
          var num = 0.0
          var sum = 0
          for(i <- x._2){
            sum = sum + i
            num = num + 1
          }
          val avg = sum/num
          val format = f"$avg%1.2f".toDouble
          (x._1,format)
        }).collect()
      .foreach(x => println(x._1 + "\t" + x._2))

    sc.stop()


  }
}
