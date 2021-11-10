package com.fst

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo2WordCountSubmit {
  def main(args: Array[String]): Unit = {

    /**
     * 将代码提交到集群上运行
     *
     * 1、去除setMaster(""local)
     * 2、修改文件的输入输出路径(因为提交到集群默认是从HDFS中获取数据,需要改成HDFS中的路径)
     * 3、在HDFS中创建目录
     * hadoop dfs -mkdir -p /spark/data/words/
     * 5、将程序打成jar包
     *  4、将数据上传至HDFS
     * hadoop dfs -put /usr/local/data/words.txt /spark/data/words/
     * 6、将jar包上传至虚拟机，然后通过spark-submit提交任务
     * spark-submit --class com.fst.Demo2WordCountSubmit --master yarn-client Spark-1.0-SNAPSHOT.jar
     * spark-submit --class com.fst.Demo2WordCountSubmit --master yarn-cluster Spark-1.0-SNAPSHOT.jar
     *
     */

    //Spark配置文件对象
    val conf: SparkConf = new SparkConf()

    // 设置Spark程序的名字
    conf.setAppName("Demo2WordCountSubmit")

    // 设置运行模式为Local模式  即在IDEA本地运行
//    conf.setMaster("local")

    // Spark的上下文环境，相当于是Spark的入口
    val sc: SparkContext = new SparkContext(conf)

    // 词频统计
    // 1、读取文件
    /**
     * RDD:弹性分布式数据集
     */
    val linesRDD: RDD[String] = sc.textFile("/spark/data/words")
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

    //使用HDFS的JAVA API判断输出路径是否已经存在，存在即删除
    val hdfsConf: Configuration = new Configuration()
    hdfsConf.set("fs.defaultFs","hdfs://master:9000")
    val fs: FileSystem = FileSystem.get(hdfsConf)
    //判断输出路径是否存在
    if(fs.exists(new Path("/spark/data/wordCount"))){
      fs.delete(new Path("/spark/data/wordCount"),true)
    }
      //5.将结果进行保存
    countRDD.saveAsTextFile("/spark/data/wordCount")



  }

}
