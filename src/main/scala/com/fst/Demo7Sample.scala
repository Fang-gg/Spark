package com.fst

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo7Sample {
  def main(args: Array[String]): Unit = {
    /**
     * sample:对数据取样
     */

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Demo7Sample")

    val sc: SparkContext = new SparkContext(conf)

    //读取学生数据构建RDD
    val stuRDD: RDD[String] = sc.textFile("Spark/data/students.txt")

    val sampleStuRDD: RDD[String] = stuRDD.sample(false, 0.1)

    sampleStuRDD.foreach(println)
  }

}
