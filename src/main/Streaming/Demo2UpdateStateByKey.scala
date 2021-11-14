import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

object Demo2UpdateStateByKey {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[2]") //接受数据需要占用一个线程
    conf.setAppName("Demo2UpdateStateByKey")

    val sc: SparkContext = new SparkContext(conf)

    // 创建StreamingContext的环境，设置处理时间间隔为5s
    // 相当于5s会封装一个batch
    val ssc: StreamingContext = new StreamingContext(sc, Durations.seconds(5))

    // 使用有状态算子需要设置checkpoint地址
    ssc.checkpoint("Spark/data/stream_checkpoint")

    /**
     * 使用nc工具模拟消息队列
     * 在linux中安装：yum install nc
     * nc -lk 8888
     *
     */
    // 通过socket连接到nc服务，得到DStream
    val words: ReceiverInputDStream[String] = ssc.socketTextStream("master", 8888)


    val wordDS: DStream[(String, Int)] = words
      .flatMap(_.split(","))
      .map(word => (word, 1))

    /**
     * reduceByKey 每隔5s算一次 前面的结果不会保留
     * 状态：前面计算的结果
     * updateStateByKey：有状态算子
     */

    /**
     *
     * @param seq    ：当前batch按key进行分组后的所有的value
     * @param option ：之前计算的结果
     * @return
     */

    def updateFunc(seq: Seq[Int], option: Option[Int]): Option[Int] = {
      val seq_sum: Int = seq.sum // 当前batch的word个数
      val last_res: Int = option.getOrElse(0) // 之前的计算结果
      Some(seq_sum + last_res)
    }

    wordDS
      .updateStateByKey(updateFunc)
      .print()

    // SparkStreaming程序需要手动启动
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()





  }
}
