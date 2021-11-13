import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Demo3DFAPI {
  def main(args: Array[String]): Unit = {
    // 创建Spark SQL入口
    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Demo3DFAPI")
      .config("spark.sql.shuffle.partitions", 2)
      .getOrCreate()

    // 导入隐式转换
    import spark.implicits._

    val stuDF: DataFrame = spark
      .read
      .format("csv")
      .schema("id String,name String,age String,gender String,clazz String")
      .load("spark/data/students.txt")

    // 对多次使用的DF也可以进行cache
    stuDF.cache()

    // show 将数据打印 默认显示20条
//        stuDF.show()
//        stuDF.show(10)
//        stuDF.show(10, truncate = false) // 完整显示

    // 过滤 where
    // 过滤出 年龄 大于 23的学生
    // DSL
    // 字符串表达式

    stuDF
      .where("age >23")
//      .show()

    // 列表达式(推荐)
    stuDF.where($"age">23)
//      .show()

    // 使用Filter加函数的方式进行过滤
    val filterDs: Dataset[Row] = stuDF.filter(row => {
      val age: String = row.getAs[String]("age")
      if (age.toInt > 23)
        true
      else
        false
    })

//    filterDs.show()

    // select
    stuDF
      .select($"id",$"name",$"clazz")
//      .show()


    // 分组 groupBy
    // 聚合
    // 统计班级人数

    stuDF
      .groupBy($"clazz")
      .count()
//      .show()


    // 导入所有的sql函数
    import org.apache.spark.sql.functions._
    //统计每个班的性别人数
    stuDF.groupBy($"clazz", $"gender")
      .agg(count($"gender"))
//          .show()

    // 统计班级人数(数据可能有重复)
    stuDF.groupBy($"clazz")
    // 去重再count
      .agg(countDistinct($"id"))
//      .show()

    // SQL 的方式
    stuDF.createOrReplaceTempView("stu")
    spark.sql(
      """
        |select
        | clazz,count(distinct id) as id_dst
        | from stu
        | group by clazz
        |
        |""".stripMargin
    )
//      .show()


    // 用完记得释放缓存
    stuDF.unpersist()



  }
}
