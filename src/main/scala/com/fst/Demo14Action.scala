package com.fst

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo14Action {
  def main(args: Array[String]): Unit = {
    /**
     * 操作算子(行为算子)
     * foreach、count、take、collect、reduce、saveAsTextFile
     */

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Demo14Action")

    val sc: SparkContext = new SparkContext(conf)

    // 读取学生数据构建RDD
    val stuRDD: RDD[String] = sc.textFile("Spark/data/students.txt")

    // foreach
    stuRDD.foreach(println)

    // take 取出前n条数据 相当于limit
    stuRDD.take(100).foreach(println)

    // count
    // 返回RDD的数据量的多少
    val l: Long = stuRDD.count()
    println(l)

    // collect
    // 将RDD转换为Scala中的Array
    // 注意数据量的大小 容易OOM
    val stuArr: Array[String] = stuRDD.collect()
    stuArr.take(10).foreach(println)

    // reduce 全局聚合
    // select sum(age) from student group by 1
    val i: Int = stuRDD.map(line => line.split(",")(2).toInt)
      .reduce(_ + _)
    println(i)

    // save
    stuRDD
      .sample(withReplacement = false, fraction = 0.2)
      .saveAsTextFile("Spark/data/sample")

  }

}
