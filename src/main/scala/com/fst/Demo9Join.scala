package com.fst

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo9Join {
  def main(args: Array[String]): Unit = {
    /**
     * join 同MySQL中的join操作类似
     * 将两个k-v格式的RDD，按照相同的key做类似于 inner join的操作
     */

    val conf: SparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Demo9Join")

    val sc: SparkContext = new SparkContext(conf)

    // 构建K-V格式的RDD
    val tuple2RDD1: RDD[(String, String)] = sc.parallelize(List(("001", "张三"), "002" -> "小红", "003" -> "小明"))
    val tuple2RDD2: RDD[(String, Int)] = sc.parallelize(List(("001", 20), "002" -> 22, "003" -> 21))
    val tuple2RDD3: RDD[(String, String)] = sc.parallelize(List(("001", "男"), "002" -> "女"))

    val joinRDD: RDD[(String, (String, Int))] = tuple2RDD1.join(tuple2RDD2)

    //    joinRDD.foreach(println)

    //    (003,(小明,21))
    //    (002,(小红,22))
    //    (001,(张三,20))

    joinRDD.map(kv => {
      val id: String = kv._1
      val name: String = kv._2._1
      val age: Int = kv._2._2
      id + "," + name + "," + age
    }).foreach(println)
    //    003,小明,21
    //    002,小红,22
    //    001,张三,20

    // 使用match接收RDD中的每一条数据
    joinRDD.map {
      case (id: String, (name: String, age: Int)) => id + "|" + name + "|" + age
    }.foreach(println)

    //    003|小明|21
    //    002|小红|22
    //    001|张三|20

    tuple2RDD1
      .leftOuterJoin(tuple2RDD3)
      .map {
        // 关联上的处理逻辑
        case (id: String, (name: String, Some(gender))) =>
          id + "," + name + "," + gender
        // 未关联上的处理逻辑
        case (id: String, (name: String, None)) =>
          id + "," + name + "," + "-"
      }.foreach(println)

//    003,小明,-
//    002,小红,女
//    001,张三,男


  }
}
