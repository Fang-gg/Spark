package com.fst

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Demo18Broadcast {
  def main(args: Array[String]): Unit = {
    /**
     * 广播变量
     */
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Demo18Broadcast")

    val sc: SparkContext = new SparkContext(conf)

    // 读取学生数据构建RDD
    val stuRDD: RDD[String] = sc.textFile("spark/data/students.txt")

    // 以学生id构成的List
    val stuIDs: List[String] = List("1500100003", "1500100013", "1500100023", "1500100033")

    // 根据stuIDs在stuRDD中过滤出这些学生的信息
    // 算子内部的代码会被封装成task
    // 相当于每个task中都有一份stuIDs 很明显造成了资源浪费
    stuRDD.filter(line => {
      val id: String = line.split(",")(0)
      stuIDs.contains(id)
    }).foreach(println)

    // 使用广播变量
    // 在Driver端 将stuIDs广播到每一个Executor中
    val stuIDsBro: Broadcast[List[String]] = sc.broadcast(stuIDs)
    stuRDD.filter(line => {
      val id: String = line.split(",")(0)
      // 从Executor中获取广播的变量
      val stuIDsV: List[String] = stuIDsBro.value
      stuIDsV.contains(id)
    }).foreach(println)

  }
}