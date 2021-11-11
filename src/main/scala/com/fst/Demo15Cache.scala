package com.fst

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Demo15Cache {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Demo15Cache")

    val sc: SparkContext = new SparkContext(conf)

    // 读取学生数据构建RDD
    val stuRDD: RDD[String] = sc.textFile("Spark/data/students.txt")

    val mapStuRDD: RDD[String] = stuRDD.map(line => {
      println("=====stu=====")
      line
    })
    // 对使用了多次的RDD进行缓存
    // cache() 默认将数据缓存到内存当中
    //    mapStuRDD.cache()

    // 如果想要选择其他的缓存策略 可以通过persist方法手动传入一个StorageLevel
    mapStuRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)

    //统计班级人数
    val clazzRDD: RDD[(String, Int)] = mapStuRDD.map(line => (line.split(",")(4), 1))
    val clazzCnt: RDD[(String, Int)] = clazzRDD.reduceByKey((x, y) => x + y)
    clazzCnt.foreach(println)

    // 统计性别人数
    mapStuRDD.map(line=>(line.split(",")(3),1))
      .reduceByKey((x,y)=>x+y)
      .foreach(println)

    // 用完记得释放缓存
    mapStuRDD.unpersist()

  }

}
