package com.fst

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo6Filter {
  def main(args: Array[String]): Unit = {
    /**
     * filter算子：主要用于过滤数据
     * 接收一个函数f，函数f需要返回一个布尔值，为true则保留，false则过滤
     */

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Demo6Filter")

    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7))

    //过滤出奇数
    listRDD.filter(i => i % 2 == 1).foreach(println)
  }

}
