package com.fst

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo4FlatMap {
  def main(args: Array[String]): Unit = {
    /**
     * flatMap:传入一条返回N条
     * 需要接收一个函数f,会将RDD中的每一条数据传给函数f,函数f处理完后需要返回一个集合或者数组
     * flatMap会自动将结果进行扁平化处理(展开)
     */

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Demo4FlatMap")

    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[String] = sc.parallelize(List("java,scala,python", "hadoop,hbase,hive"))

    listRDD.flatMap(line=>{
      line.split(",")
    }).foreach(println)

  }

}
