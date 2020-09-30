package chapter3.part4

import org.apache.spark.sql.functions.{count, desc, lit}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("网页被访问次数分布统计")
                                          .getOrCreate()
    val data: DataFrame = spark.read.table("law.law_visit_log_all")
    val fullurl = "fullurl"
    val webNum: Long = data.select(fullurl).distinct.count
    val webDistribute: Dataset[Row] =
      data.groupBy(fullurl)
          .agg(count(lit(1)) as "webCount")
          .groupBy("webCount")
          .agg(
            count(fullurl) as "count",
            count(fullurl) * 100.0 / webNum as "count_percent",
            )
          .orderBy(desc("count"))
    webDistribute.show(false)
  }
}
