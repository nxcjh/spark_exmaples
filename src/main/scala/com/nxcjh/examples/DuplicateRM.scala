package com.nxcjh.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 数据去重问题
  *
  * 原始数据
  * file1 :
  * 2012-3-1 a
  * 2012-3-2 b
  * 2012-3-3 c
  * 2012-3-4 d
  * 2012-3-5 a
  * 2012-3-6 b
  * 2012-3-7 c
  * 2012-3-3 c
  *
  * file2:
  * 2012-3-1 b
  * 2012-3-2 a
  * 2012-3-3 b
  * 2012-3-4 d
  * 2012-3-5 a
  * 2012-3-6 c
  * 2012-3-7 d
  * 2012-3-3 c
  *
  * 说明: 数据去重的最终目标是让原始数据中出现次数超过一次的数据在输出文件中只出现一次.
  * 我们自然而然会想到将同一个数据的所有记录都交给一台reduce机器.
  *
  * 无论这个数据出现多少次, 只要在最终结果中输出一次就可以了. 具体就是reduce的输入应该
  * 以数据作为key. 而对value-list则 没有要求. 当reduce 接收到一个<key,value-list>
  * 时就直接将key复制到输出的key中, 并将value设置成空值.
  *
  * Created by aimei02132 on 17/1/11.
  */
object DuplicateRM {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DuplicateRM").setMaster("local")
    val sc = new SparkContext(conf)

    val contests = sc.textFile("/spark_input/two")

    contests
      .filter(_.trim.length>0)
      .map(line => (line.trim,""))
      .groupByKey
      .sortByKey().keys
      .collect
      .foreach(println _)

    sc.stop()
  }
}
