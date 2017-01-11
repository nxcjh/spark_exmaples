package com.nxcjh.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 日志分析: 分析非结构化文件
  *
  * 根据tomcat日志计算URL访问情况, 具体的URL如下:
  * 要求: 分别统计GET和POST访问量
  * 结果为: 访问方式, URL, 访问量
  *
  * 原始数据:
  * 196.168.2.1 - - [03/Jul/2014:23:36:38 +0800] "GET /course/detail/3.htm HTTP/1.0" 200 38435 0.038
  * 182.131.89.195 - - [03/Jul/2014:23:37:43 +0800] "GET /html/notes/20140617/888.html HTTP/1.0" 301 - 0.000
  * 196.168.2.1 - - [03/Jul/2014:23:38:27 +0800] "POST /service/notes/addViewTimes_23.htm HTTP/1.0" 200 2 0.003
  * 196.168.2.1 - - [03/Jul/2014:23:39:03 +0800] "GET /html/notes/20140617/779.html HTTP/1.0" 200 69539 0.046
  * 196.168.2.1 - - [03/Jul/2014:23:43:00 +0800] "GET /html/notes/20140318/24.html HTTP/1.0" 200 67171 0.049
  * 196.168.2.1 - - [03/Jul/2014:23:43:59 +0800] "POST /service/notes/addViewTimes_779.htm HTTP/1.0" 200 1 0.003
  * 196.168.2.1 - - [03/Jul/2014:23:45:51 +0800] "GET /html/notes/20140617/888.html HTTP/1.0" 200 70044 0.060
  * 196.168.2.1 - - [03/Jul/2014:23:46:17 +0800] "GET /course/list/73.htm HTTP/1.0" 200 12125 0.010
  * 196.168.2.1 - - [03/Jul/2014:23:46:58 +0800] "GET /html/notes/20140609/542.html HTTP/1.0" 200 94971 0.077
  * 196.168.2.1 - - [03/Jul/2014:23:48:31 +0800] "POST /service/notes/addViewTimes_24.htm HTTP/1.0" 200 2 0.003
  * 196.168.2.1 - - [03/Jul/2014:23:48:34 +0800] "POST /service/notes/addViewTimes_542.htm HTTP/1.0" 200 2 0.003
  * 196.168.2.1 - - [03/Jul/2014:23:49:31 +0800] "GET /notes/index-top-3.htm HTTP/1.0" 200 53494 0.041
  * 196.168.2.1 - - [03/Jul/2014:23:50:55 +0800] "GET /html/notes/20140609/544.html HTTP/1.0" 200 183694 0.076
  * 196.168.2.1 - - [03/Jul/2014:23:53:32 +0800] "POST /service/notes/addViewTimes_544.htm HTTP/1.0" 200 2 0.004
  * 196.168.2.1 - - [03/Jul/2014:23:54:53 +0800] "GET /service/notes/addViewTimes_900.htm HTTP/1.0" 200 151770 0.054
  * 196.168.2.1 - - [03/Jul/2014:23:57:42 +0800] "GET /html/notes/20140620/872.html HTTP/1.0" 200 52373 0.034
  * 196.168.2.1 - - [03/Jul/2014:23:58:17 +0800] "POST /service/notes/addViewTimes_900.htm HTTP/1.0" 200 2 0.003
  * 196.168.2.1 - - [03/Jul/2014:23:58:51 +0800] "GET /html/notes/20140617/888.html HTTP/1.0" 200 70044 0.057
  * 186.76.76.76 - - [03/Jul/2014:23:48:34 +0800] "POST /service/notes/addViewTimes_542.htm HTTP/1.0" 200 2 0.003
  * 186.76.76.76 - - [03/Jul/2014:23:46:17 +0800] "GET /course/list/73.htm HTTP/1.0" 200 12125 0.010
  * 8.8.8.8 - - [03/Jul/2014:23:46:58 +0800] "GET /html/notes/20140609/542.html HTTP/1.0" 200 94971 0.077
  *
  *
  *
  * Created by aimei02132 on 17/1/11.
  */
object LogLearn {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("LogLearn").setMaster("local")
    val sc = new SparkContext(conf)

    val contests = sc.textFile("/spark_input/seven")

    //过滤长度小于0, 过滤不包含GET/POST的URL
    val filtered = contests.filter(_.trim.length>0)
      .filter(line => (line.indexOf("GET") > 0) || (line.indexOf("POST") > 0))

    //转换成键值对操作
    val res = filtered.map( line => {
      if(line.indexOf("GET") > 0){//截取GET到URL的字符串
        (line.substring(line.indexOf("GET"),line.indexOf("HTTP/1.0")).trim,1)
      }else{ //截取POST 到URL 的字符串
        (line.substring(line.indexOf("POST"),line.indexOf("HTTP/1.0")).trim,1)
      }
    }).reduceByKey(_+_)
    res.collect.foreach(println _)
  }
}
