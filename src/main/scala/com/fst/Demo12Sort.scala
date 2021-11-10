package com.fst

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo12Sort {
  def main(args: Array[String]): Unit = {
    /**
     * sortBy 转换算子
     * 指定按什么排序 默认升序
     *
     * sortByKey 转换算子
     * 需要作用在KV格式的RDD上，直接按key排序 默认升序
     */

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Demo12Sort")

    val sc: SparkContext = new SparkContext(conf)

    // 读取学生数据构建RDD
    val stuRDD: RDD[String] = sc.textFile("Spark/data/students.txt")

    // ascending = false 降序排列
    stuRDD.sortBy(line=>line.split(",")(0),ascending = false).foreach(println)


    stuRDD.map(line => (line.split(",")(0), line))
      .sortByKey(ascending = false)
      .foreach(println)
  }

}
