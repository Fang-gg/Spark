import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object Demo1WordCountStreaming {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[2]")  //接收数据需要占用一个线程
    conf.setAppName("Demo1WordCountStreaming")

    val sc: SparkContext = new SparkContext(conf)

    //创建StreamingContext的环境，设置处理时间间隔为5s
    // 相当于5s会封装一个batch
    val ssc: StreamingContext = new StreamingContext(sc, Durations.seconds(5))

    /**
     * 使用nc工具模拟消息队列
     * 在linux中安装：yum install nc
     * nc -lk 8888
     *
     */

    // 通过socket连接到nc服务，得到DStream
    val words: ReceiverInputDStream[String] = ssc.socketTextStream("master", 8888)

//        words.print() // 打印

    val wordsDS: DStream[String] = words.flatMap(word => word.split(","))
    wordsDS.map(word=>(word,1))
      .reduceByKey(_+_)
      .print()


    // SparkStreaming程序需要手动启动
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }

}
