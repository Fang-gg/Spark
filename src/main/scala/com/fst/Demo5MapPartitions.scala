package com.fst

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo5MapPartitions {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Demo5MapPartitions")

    val sc: SparkContext = new SparkContext(conf)

    //读取学生数据构建RDD
    val stuRDD: RDD[String] = sc.textFile("Spark/data/students.txt", 6)
    println(stuRDD.getNumPartitions) //这不是算子，只是RDD中的一个属性

    //take  也是action的一个算子  会返回一个Array
    // 这里的foreach实际上是Array的方法，不是RDD的算子
    stuRDD.take(10).foreach(println)

    //mapPartitions也是一个转换算子
    stuRDD.mapPartitions(rdd => {
      println("map partitions")
      //按分区去处理数据
      rdd.map(line => line.split(",")(1))
    }).foreach(println)

    // foreachPartition也是一个操作算子
    stuRDD.foreachPartition(rdd => {
      println("map partitions")
      //按分区去处理数据
      rdd.map(line => line.split(",")(1)).take(10).foreach(println)
    })

  }
}
