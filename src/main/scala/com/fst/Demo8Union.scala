package com.fst

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo8Union {
  def main(args: Array[String]): Unit = {
    /**
     * union:将两个RDD首尾相连变成一个RDD
     * 两个RDD的结构必须一样
     */
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Demo8Union")

    val sc: SparkContext = new SparkContext(conf)

    val listRDD1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7))
    println(listRDD1.getNumPartitions)

    val listRDD2: RDD[Int] = sc.parallelize(List(8,9,10,11))
    println(listRDD2.getNumPartitions)

    val unionRDD: RDD[Int] = listRDD1.union(listRDD2)
    println(unionRDD.getNumPartitions)

    unionRDD.foreach(println)
  }
}
