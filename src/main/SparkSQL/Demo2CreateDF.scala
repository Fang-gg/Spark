import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.lang

object Demo2CreateDF {
  def main(args: Array[String]): Unit = {
    // 创建Spark SQL入口
    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Demo2CreateDF")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()

    /**
     * 1、读json数据
     */

    val jsonDF: DataFrame = spark
      .read
      .format("json")
      .load("Spark/data/students.json")

//    jsonDF.show() // 默认显示20条
//    jsonDF.show(10)  // 显示10条数据
//    jsonDF.show(false)  // 完全显示

    /**
     * 2、读文本文件
     */

    spark
      .read
      .format("csv")
      .option("sep",",")  // 指定分隔符
      .schema("id String,name String,age Int,gender String,clazz String")
      .load("Spark/data/students.txt")
//      .show()

    /**
     * 3、JDBC 读取MySQL的一张表转换成 Spark SQL 中的DF
     */

    val jdbcDF: DataFrame = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://rm-bp1h7v927zia3t8iwho.mysql.rds.aliyuncs.com:3306/students")
      .option("dbtable", "students")
      .option("user", "shujia012")
      .option("password", "123456")
      .load()

//    jdbcDF.show()

    // 将数据以parquet格式保存
//     jdbcDF.write.parquet("Spark/data/stu_parquet")

    /**
     * 4、读取parquet文件
     * 无法直接查看，默认会进行压缩，而且自带表结构，读取时不需要指定schema
     * 默认使用snappy压缩方式进行压缩
     */

    spark
      .read
      .format("parquet")
      .load("Spark/data/stu_parquet")
//      .show(10)

    // 将数据以orc格式保存
//    jdbcDF.write.orc("Spark/data/stu_orc")

    /**
     * 5、读取ORC格式的文件
     * 也会默认进行压缩，空间占用率最小，默认带有表结构，可以直接读取
     */

    spark
      .read
      .format("orc")
      .load("Spark/data/stu_orc")
//      .show(10)


    /**
     * 6、从RDD构建DF
     */

    val stuRDD: RDD[String] = spark.sparkContext.textFile("Spark/data/students.txt")

    val tupleRDD: RDD[Student] = stuRDD.map(line => {
      val splits: Array[String] = line.split(",")
      val id: String = splits(0)
      val name: String = splits(1)
      val age: String = splits(2)
      val gender: String = splits(3)
      val clazz: String = splits(4)
      Student(id, name, age, gender, clazz)
    })

    // 导入隐式转换
    import spark.implicits._
    val sDF: DataFrame = tupleRDD.toDF()
//    sDF.show(10)

    // DataFrame to RDD
    val rdd: RDD[Row] = sDF.rdd
    rdd.foreach(row => {
      val id: String = row.getAs[String]("id")
      val name: String = row.getAs[String]("name")
      println(s"$id,$name")
    })

  }

  case class Student(id:String,name:String,age:String,gender:String,clazz:String)
}
