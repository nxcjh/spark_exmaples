package com.nxcjh.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * 求最大的K个值并排序
  *
  * 需求分析
  * #orderid, userid, payment, productid
  * 求topN的payment值
  *
  * file1
  * 100,3333,10,100
  * 101,9321,1000,293
  * 102,3881,701,20
  * 103,6791,910,30
  * 104,8888,11,39
  *
  *
  * file2
  * 1,9819,100,121
  * 2,8918,2000,111
  * 3,2813,1234,22
  * 4,9100,10,1101
  * 5,3210,490,111
  * 6,1298,28,1211
  * 7,1010,281,90
  * 8,1818,9000,20
  *
  * 实例结果
  * 1	9000
  * 2	2000
  * 3	1234
  * 4	1000
  * 5	910
  *
  * Created by aimei02132 on 17/1/11.
  */
object DataKTopSort {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DataKTopSort").setMaster("local")
    val sc = new SparkContext(conf)

    val contests = sc.textFile("/spark_input/six")
    var idx = 0
    val res = contests.filter(x => (x.trim.length>0) && (x.trim.split(",").length ==4))
      .map(_.split(",")(2))
      .map(x => (x.toInt,""))
      .sortByKey(false)
      .map(x=>x._1)
      .take(5)
      .foreach(x => {
        idx = idx + 1
        println(idx + "\t" + x)
      })

  }
}
