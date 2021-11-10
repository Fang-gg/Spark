package com.fst

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Demo13MapValues {
  def main(args: Array[String]): Unit = {
    /**
     * mapValues 转换算子
     * 需要作用在K—V格式的RDD上
     * 传入一个函数f
     * 将RDD的每一条数据的value传给函数f，key保持不变
     * 数据规模也不会改变
     */
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Demo13MapValues")

    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[(String, Int)] = sc.parallelize(List(("张三", 1), ("李四", 2), ("王五", 3)))
    listRDD.mapValues(value => value * value)
      .foreach(println)
  }

}
