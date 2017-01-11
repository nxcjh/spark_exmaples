package com.nxcjh.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * 数据排序
  *
  * 1."数据排序"是许多实际任务执行时要完成的第一项工作，
  * 比如学生成绩评比、数据建立索引等。这个实例和数据去重类似，都是先对原始数据进行初步处理，为进一步的数据操作打好基础。
  *
  * 需求描述:
  * 对输入文件中数据进行排序。输入文件中的每行内容均为一个数字，即一个数据。
  * 要求在输出中每行有两个间隔的数字，其中，第一个代表原始数据在原始数据集中的位次，第二个代表原始数据。
  *
  *
  * 输入文件
  *
  * file1:
  * 2
  * 32
  * 654
  * 32
  * 15
  * 756
  * 65223
  *
  *
  * file2:
  * 5956
  * 22
  * 650
  * 92
  *
  *
  * file3:
  * 26
  * 54
  * 6
  *
  * 样例输出:
  * 1    2
  * 2    6
  * 3    15
  * 4    22
  * 5    26
  * 6    32
  * 7    32
  * 8    54
  * 9    92
  * 10    650
  * 11    654
  * 12    756
  * 13    5956
  * 14    65223
  *
  * 设计思考
  * 这个实例仅仅要求对输入数据进行排序, 熟悉MapReduce过程的读者会很快想到在MapReduce过程中就有排序,
  * 是否可以利用这个默认的排序, 而不需要自己再实现具体的排序呢? 答案是肯定的.
  *
  * 但是在使用之前首先要了解它的默认排序规则. 它是按照key值进行排序的, 如果key为封装int的IntWritable类型,
  * 那么MapReduce按照数字大小对key排序. 如果key为封装为String的Text类型, 那么MapReduce按照字典顺序对
  * 字符串进行排序.
  *
  * 了解了这个细节, 我们就知道应该使用封装int的IntWritable型数据结构了. 也就是在map中将读入的数据转化成IntWritable型.
  * 然后作为key值输出(value任意).
  *
  * reduce拿到<key,value-list> 之后, 将输入的key作为value输出, 并根据value-list中元素的个数决定输出的次数.
  * 输出的key(即代码中的linenum)是一个全局变量, 它统计当前key的位次. 需要注意的是这个程序没有配置Combiner, 也就是在
  * MapReduce过程中不适用Combiner. 这主要是因为使用map和reduce就已经能够完成任务了.
  *
  *
  *
  *
  *
  * Created by aimei02132 on 17/1/11.
  */
object DataSort {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DataSort").setMaster("local")
    val sc = new SparkContext(conf)

    val three = sc.textFile("/spark_input/three/",3)

    var idx = 0
    import org.apache.spark.HashPartitioner

    val res = three.filter(_.trim.length>0)
        .map(num => (num.trim.toInt,""))
        .partitionBy(new HashPartitioner(1))
        .sortByKey().map(t => {
            idx += 1
            (idx,t._1)
        })
        .collect
        .foreach(x => println(x._1 + "\t" + x._2))

    sc.stop()

    // 由于输入文件有多个, 产生不同的分区, 为了生成序号, 使用HashPartitioner将中间的RDD规约到一起

  }
}
