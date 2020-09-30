package chapter3.part2

import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("统计数据日流量、月流量")
      .getOrCreate()
    val data: DataFrame = spark.sql("SELECT * FROM law.law_visit_log_all")
    //统计日访问流量
    data.select("ymd").groupBy("ymd")
        .agg(count(lit(1)) as "day_visits")
        .orderBy("ymd").show(10)
    // 统计日用户流量
    data.select("ymd", "userid")
        .distinct.groupBy("ymd")
        .agg(count(lit(1)) as "day_users")
        .orderBy("ymd").show(10)
  }
}
