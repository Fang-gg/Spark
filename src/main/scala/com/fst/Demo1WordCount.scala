package com.fst

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo1WordCount {
  def main(args: Array[String]): Unit = {

    //Spark配置文件对象
    val conf: SparkConf = new SparkConf()

    // 设置Spark程序的名字
    conf.setAppName("Demo1WordCount")

    // 设置运行模式为Local模式  即在IDEA本地运行
    conf.setMaster("local")

    // Spark的上下文环境，相当于是Spark的入口
    val sc: SparkContext = new SparkContext(conf)

    // 词频统计
    // 1、读取文件
    /**
     * RDD:弹性分布式数据集
     */
    val linesRDD: RDD[String] = sc.textFile("Spark/data/words.txt")
//    linesRDD.foreach(println) // 打印出来看看

    // 2、将每一行的单词切分出来
    // flatMap: 在Spark中称为  算子
    // 算子一般情况下都会返回另外一个RDD
    val wordsRDD: RDD[String] = linesRDD.flatMap(line => line.split(","))
//    wordsRDD.foreach(println)  // 打印出来看看

    // 3、按照单词分组
    val groupRDD: RDD[(String, Iterable[String])] = wordsRDD.groupBy(word => word)
//    groupRDD.foreach(println)  // 打印出来看看

    // 4、统计每个单词的数量
    val countRDD: RDD[String] = groupRDD.map(kv => {
      val word: String = kv._1
      val words: Iterable[String] = kv._2

      // words.size 直接获取迭代器的大小
      // 因为相同分组的所有的单词都会到迭代器中
      // 所以迭代器的大小就是单词的数量
      word + "," + words.size
    })

      //5.将结果进行保存
    countRDD.saveAsTextFile("Spark/data/wordCount")



  }

}
