package com.fst

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Demo16CheckPoint {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Demo16CheckPoint")

    val sc: SparkContext = new SparkContext(conf)

    // 设置CheckPoint 存储的位置
    sc.setCheckpointDir("Spark/data/checkpoint")

    // 读取学生数据构建RDD
    val stuRDD: RDD[String] = sc.textFile("Spark/data/students.txt")

    val mapStuRDD: RDD[String] = stuRDD.map(line => {
      println("=====stu=====")
      line
    })
    // 对使用了多次的RDD进行缓存
    // cache() 默认将数据缓存到内存当中
    //        mapStuRDD.cache()


    // 如果想要选择其他的缓存策略 可以通过persist方法手动传入一个StorageLevel
//    mapStuRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // 因为checkpoint需要重新启动一个任务进行计算并写入HDFS
    // 可以在checkpoint之前 先做一次cache 可以省略计算过程 直接写入HDFS

    mapStuRDD.cache()
    // 将数据缓存到 HDFS
    mapStuRDD.checkpoint()
    // checkpoint主要运用在SparkStreaming中的容错

    //统计班级人数
    val clazzRDD: RDD[(String, Int)] = mapStuRDD.map(line => (line.split(",")(4), 1))
    val clazzCnt: RDD[(String, Int)] = clazzRDD.reduceByKey((x, y) => x + y)
    clazzCnt.foreach(println)

    // 统计性别人数
    mapStuRDD.map(line=>(line.split(",")(3),1))
      .reduceByKey((x,y)=>x+y)
      .foreach(println)

    // 用完记得释放缓存
//    mapStuRDD.unpersist()

  }

}
