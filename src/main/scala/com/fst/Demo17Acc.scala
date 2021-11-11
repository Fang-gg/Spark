package com.fst

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Demo17Acc {
  def main(args: Array[String]): Unit = {
    /**
     * 累加器
     */
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Demo17Acc")

    val sc: SparkContext = new SparkContext(conf)

    // 读取学生数据构建RDD
    val stuRDD: RDD[String] = sc.textFile("spark/data/students.txt")

    var i: Int = 0
    println("i is " + i)
    // 算子内部的代码 使用了外部变量i
    // 实际上最终封装到task中的是i的一个副本
    stuRDD.foreach(line => {
      i += 1
      println(line)
    })
    println("i is " + i)

    // 如果想在算子内部 对外部的变量做一个累加操作
    // 累加器
    // 在算子外面 即Driver端 通过累加器创建一个变量l
    val l: LongAccumulator = sc.longAccumulator
    stuRDD.foreach(line => {
      // 在算子内部使用累加器进行累加
      l.add(1)
      println(line)
    })
    // 在算子外面 即Driver端 获取累加器最终的结果
    println(l.value)

    // RDD内部不能再套RDD
    /**
     * 首先RDD是一种抽象的编程模型，并没有实现序列化所以不能进行网络传输
     * 其次就算RDD能够进行网络传输，那如果RDD中还有RDD，
     * 那么需要再task再去申请资源启动Driver、Executor？
     *
     * 如果RDD中套了RDD 就要去整理一下思路，是不是可以转换为其他方式去实现你的逻辑
     */
    //    stuRDD.foreach(line=>{
    //      stuRDD.foreach(l2=>{
    //        println(l2)
    //      })
    //    })

  }
}