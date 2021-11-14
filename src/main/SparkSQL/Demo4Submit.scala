import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Demo4Submit {
  def main(args: Array[String]): Unit = {
    /**
     * SparkSession 是Spark 2.0引入的新的入口
     */
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Demo4Submit")
      // 设置spark sql产生shuffle后默认的分区数 => 并行度
      // 默认是 200
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 读取HDFS的数据
    val stuCsvDF: DataFrame = spark
      .read
      // 手动指定列名
      .schema("id String,name String,age Int,gender String,clazz String")
      .csv("/spark/data/stu/input/")

    //统计班级人数 并将最后结果写入HDFS
    stuCsvDF
      .groupBy($"clazz")
      .count()
      .write
      .format("csv")
      .option("sep","\t")
      .mode(SaveMode.Overwrite)
      .save("/spark/data/stu/output/")


    /**
     * 1、在HDFS中创建目录
     * hdfs dfs -mkdir -p /spark/data/stu/input/
     * 2、将students.txt 数据上传至/spark/data/stu/input/
     * hdfs dfs -put students.txt /spark/data/stu/input/
     * 3、将代码打包上传到虚拟机，通过命令提交
     * spark-submit --class Demo4Submit --master yarn-client Spark-1.0-SNAPSHOT.jar
     */


  }
}
