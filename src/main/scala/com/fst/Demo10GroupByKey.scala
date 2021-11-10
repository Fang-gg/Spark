package com.fst

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo10GroupByKey {
  def main(args: Array[String]): Unit = {
    /**
     * groupBy 指定分组的字段进行分组
     */

    // 统计班级人数
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Demo10GroupByKey")

    val sc: SparkContext = new SparkContext(conf)

    //读取学生数据构建RDD
    val stuRDD: RDD[String] = sc.textFile("Spark/data/students.txt")

    stuRDD.groupBy(stu => stu.split(",")(4))
      .map(kv => (kv._1, kv._2.size))
      .foreach(println)

    /**
     * groupByKey 作用K-V格式的RDD上，默认按K分组
     */
    stuRDD.map(line => (line.split(",")(4), line))
      .groupByKey()
      .map(kv => {
        val clazz: String = kv._1
        val size: Int = kv._2.size
        "(" + clazz + "," + size + ")"
      })
      .foreach(println)
  }

}
