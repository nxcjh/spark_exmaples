package com.nxcjh.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by aimei02132 on 17/1/11.
  *
  * 通过采集的气象数据分析每年的最高温度
  *
  * 原始数据分析:
  * 0067011990999991950051507004888888889999999N9+00001+9999999999999999999999
  * 0067011990999991950051512004888888889999999N9+00221+9999999999999999999999
  * 0067011990999991950051518004888888889999999N9-00111+9999999999999999999999
  * 0067011990999991949032412004888888889999999N9+01111+9999999999999999999999
  * 0067011990999991950032418004888888880500001N9+00001+9999999999999999999999
  * 0067011990999991950051507004888888880500001N9+00781+9999999999999999999999
  *
  * 数据说明:
  * 第15-19个字符表示year
  * 第45-50位表示温度, + 表示零上, - 表示零下, 且温度的值不能是9999, 9999表示异常数据
  * 第50位值只能是0, 1, 4, 5, 9
  *
  *
  */

object ComputeMax{

  //过滤脏数据
  val func = (l : String) => {
    val quality = l.substring(50,51)
    var airTemperature = 0
    if(l.charAt(45) == '+'){
      airTemperature = l.substring(46,50).toInt
    }else{
      airTemperature = l.substring(45,50).toInt
    }
    airTemperature != 9999 && quality.matches("[01459]")

  }

  //格式化数据=> (year, 气温)
  val func2 = (l: String) => {
    val year = l.substring(15,19)
    var airTemperature = 0

    if(l.charAt(45) == '+'){
      airTemperature = l.substring(46,50).toInt
    }else{
      airTemperature = l.substring(45,50).toInt
    }
    (year,airTemperature)
  }



  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ComputeMax").setMaster("local")
    val sc = new SparkContext(conf)

    val contests = sc.textFile("/spark_input/data_1")
    contests
      .filter(func)
      .map(func2)
      .reduceByKey((x,y) => if (x>y) x else y) //比较大小
      .collect
      .foreach(x => println("year: " + x._1 + ",max: " + x._2))

    sc.stop()
  }
}

/**
  * 结果
  * year: 1949,max: 111
  * year: 1950,max: 78
  */


