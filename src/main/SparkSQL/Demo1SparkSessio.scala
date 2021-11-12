import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Demo1SparkSessio {
  def main(args: Array[String]): Unit = {
    /**
     * SparkSessio 是Spark 2.0引入的新的入口
     */

    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Demo1SparkSessio")
      //设置spark sql产生shuffle后默认的分区数 => 并行度
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()

    // 从SparkSession获取SparkContext
//    val sc: SparkContext = spark.sparkContext

    val stuDF: DataFrame = spark.read.json("Spark/data/students.json")
//    stuDF.show()  // 默认展示20条数据

    // 文本类的数据 默认是没有列名的 直接读进来是 _c0 _c1 _c2 ......
    val stuCsvDF: DataFrame = spark
      .read
      //手动指定列名
      .schema("id String,name String,age Int,gender String,clazz String")
      .csv("Spark/data/students.txt")

//    stuCsvDF.show()

    // 直接将DataFrame注册成临时视图view
    stuDF.createOrReplaceTempView("stu")

    //sql的方式
    val ageDF: DataFrame = spark.sql("select * from stu where age > 23")

    // 同rdd一样，操作算子可以触发job
//        ageDF.show()

    // DSL 类SQL的方式 介于SQL和代码中间的API
    stuDF.where("age > 23")
      .select("name", "age", "clazz")
    //      .show()

    // 统计班级人数
    stuDF.groupBy("clazz")
      .count()
      .write
      // 保存的时候可以指定SaveMode
      // Overwrite 覆盖
      // Append 追加
      .mode(SaveMode.Overwrite)
      // 默认以parquet形式保存
      .save("Spark/data/clazz_cnt")
  }

}
