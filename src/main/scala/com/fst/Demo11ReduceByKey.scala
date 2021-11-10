package com.fst

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo11ReduceByKey {
  def main(args: Array[String]): Unit = {
    /**
     * reduceByKey
     */

    // 统计班级人数
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Demo11ReduceByKey")

    val sc: SparkContext = new SparkContext(conf)

    // 读取学生数据构建RDD
    val stuRDD: RDD[String] = sc.textFile("spark/data/students.txt")

    //使用groupBy的方式
    stuRDD.map(line => (line.split(",")(4), 1))
      .groupBy(_._1)
      .map(kv => {
        val clazz: String = kv._1
        val clazz_1_iter: Iterable[(String, Int)] = kv._2
        val clazz_sum: Int = clazz_1_iter.map(_._2).sum
        clazz + "," + clazz_sum
      }).foreach(println)

    /**
     * reduceByKey 需要接收一个聚合函数
     * 首先会对数据按key分组 然后在组内进行聚合（一般是加和，也可以是Max、Min之类的操作）
     * 相当于 MR 中的combiner
     * 可以在Map端进行预聚合，减少shuffle过程需要传输的数据量，以此提高效率
     * 相对于groupByKey来说，效率更高，但功能更弱
     * 幂等操作
     * y = f(x) = f(y) = f(f(x))
     *
     */

    stuRDD
      .map(line => (line.split(",")(4), 1))
      .reduceByKey((x: Int, y: Int) => x + y)
      .foreach(println)

    // 简写
    stuRDD
      .map(line => (line.split(",")(4), 1))
      .reduceByKey(_ + _)
      .foreach(println)


  }
}
