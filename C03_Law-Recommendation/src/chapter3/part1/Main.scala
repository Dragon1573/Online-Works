package chapter3.part1

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("数据的记录数、用户数、网页数")
                                          .getOrCreate()
    val data: DataFrame = spark.sql("SELECT * FROM law.law_visit_log_all")
    //统计数据的总记录数
    data.count()
    //统计数据的用户数
    data.select("userid").distinct().count()
    //统计数据的网页数
    data.select("fullurl").distinct().count()
  }
}
