package chapter3.part6

import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("动态参数网址分析")
                                          .getOrCreate()
    val data: DataFrame = spark.read.table("law_visit_log_all")
    data.select("fullurl").filter("fullurl LINK '%?%'").count
    val pageWith: DataFrame = spark.sql(
      """
        | SELECT
        |   fullurlid AS page_type,
        |   count(*) AS count_num,
        |   ROUND((COUNT(*) * 100) / 22752.0, 4) AS weights
        | FROM law.law_visit_log_all
        | WHERE fullurl like '%?%'
        | GROUP BY fullurlid
        | """.stripMargin,
      )
    pageWith.orderBy(-pageWith("weights")).show()
    data.select("fullurl", "fullurlid")
        .filter("fullurl LIKE '%?%'")
        .filter("fullurlid = 301001")
        .show(5, truncate = false)
    data.select("fullurl", "fullurlid")
        .filter("fullurl LIKE '%?%'")
        .filter("fullurlid = 107001")
        .show(5, truncate = false)
  }
}
