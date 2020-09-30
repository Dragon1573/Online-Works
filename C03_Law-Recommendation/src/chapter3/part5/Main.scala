package chapter3.part5

import org.apache.spark.sql.functions.{col, count, desc, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("用户访问页面次数统计")
                                          .getOrCreate()
    // 加载数据
    val data: DataFrame = spark.sql("SELECT * FROM law.law_visit_log_all")
    val num: Long = data.select("userid").distinct.count
    // 用户访问页面个数统计
    data.groupBy("userid").agg(count(lit(1)) as "url_times").groupBy("url_times")
        .agg(count(lit(1)) as "url_times_user_size")
        .withColumn("url_times_user_size_percent",
                    col("url_times_user_size") * 100.0 / num)
        .orderBy(desc("url_times_user_size")).show(30)
  }
}


