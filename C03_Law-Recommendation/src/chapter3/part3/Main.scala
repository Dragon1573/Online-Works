package chapter3.part3

import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("重复数据探索")
                                          .getOrCreate()
    val data: DataFrame = spark.sql("SELECT * FROM law.law_visit_log_all")
    // 统计数据的总记录数
    println(s"总记录数量为${data.count()}")
    // 统计数据不重复的总记录数
    println(s"不重复记录数量为：${data.distinct().count()}")
    // 统计重复的数据
    println(s"重复的数量为：${data.count() - data.distinct().count()}")
  }
}

