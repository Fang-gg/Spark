import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo5Hive {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Demo5Hive")
      .config("spark.sql.shuffle.partitions", 2)
      .enableHiveSupport() // 开启Hive的支持
      .getOrCreate()


    // 导入隐式转换
    import spark.implicits._
    // 导入Spark SQL所有的函数
    import org.apache.spark.sql.functions._

    spark.sql("show tables").show()

    spark.sql("use kettle")

    spark.sql("show tables").show()

    spark.sql("use default")

    spark.sql(
      """
        |select clazz,count(*) from stu1 group by clazz
        |""".stripMargin).show()

    // DSL 的方式

    val stu1DF: DataFrame = spark.table("stu1")

    stu1DF.where($"clazz" === "文科一班").show()
  }

}