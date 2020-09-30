package chapter3.part8

import org.apache.spark.sql.functions.{col, count, countDistinct}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * <h1>统计访问重复页面的用户数</h1>
 */
object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("统计访问重复页面的用户数")
                                          .getOrCreate()
    val data: DataFrame = spark.read.table("law.law_visit_log_all")
    val num: Long = data.groupBy("userid").agg(
      count("fullurl") as "all_page_count",
      countDistinct("fullurl") as "dis_page_count",
      ).filter(col("all_page_count") > col("dis_page_count")).count
    println(s"访问重复页面的用户数有${num}户")
  }
}
