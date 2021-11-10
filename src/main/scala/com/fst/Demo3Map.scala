package com.fst

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo3Map {
  def main(args: Array[String]): Unit = {
    /**
     * 转换算子:将一个RDD变成另一个RDD，RDD之间的转换，懒执行，需要操作算子触发执行
     * 操作算子(行为算子):不能将一个RDD变成另一个RDD,每一个操作算子都会触发一个job
     * 一个Spark程序中可以包含很多个job
     * 可以通过算子的返回值去判断该算子是转换算子还是操作算子
     */

    /**
     * RDD的构建方式
     * 1、通过textFile读取文件
     * 2、通过Scala中的集合创建
     */

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Demo3Map")

    val sc: SparkContext = new SparkContext(conf)

    /**
     * map:传入一条数据  返回一条数据
     * 不会改变数据的规模
     * 接收一个函数f:参数同RDD中数据的类型   返回值类型由自己决定
     * 将RDD中的每一条数据依次传给函数f
     */
      //通过List构建RDD
    val listRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7))
    val i2RDD: RDD[Int] = listRDD.map((i: Int) => {
      i * 2
    })

    //foreach也是一个操作算子 可以触发任务
    i2RDD.foreach(println)



  }

}
