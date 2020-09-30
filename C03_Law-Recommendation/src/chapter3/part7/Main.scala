package chapter3.part7

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Main {
  /**
   * <h2>判断是否为小编号</h2>
   * <p>2位数以内的编号才是页码编号，其他的编号形式属于商品页面编号。</p>
   *
   * @param num 编号字符串
   * @return 布尔判断结果
   */
  def small_int(num: String): Boolean = {
    if (StringUtils.isNumeric(num) && num.length < 3) true else false
  }

  /**
   * <h2>判断页面是否为多页链接</h2>
   *
   * @param page     完整链接
   * @param url_type 网页类型编号
   * @return 布尔判断结果
   */
  def is_next_page(page: String, url_type: String): Boolean = {
    if (!"107001".equals(url_type) || null == page || page.length < 6) {
      return false
    } else if (page.contains(".html") && page.contains("_")) {
      val second: Int = page.lastIndexOf(".")
      val _before: Int = page.lastIndexOf("_")
      if (_before >= 0 && second > _before + 1) {
        val t: String = page.substring(_before + 1, second)
        return small_int(t)
      }
    }
    false
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("翻页网址分析")
                                          .getOrCreate()
    // 是否翻页
    val next_page: UserDefinedFunction = udf(
      (page: String, url_type: String) => !is_next_page(page, url_type),
      )
    // 翻页网页统计
    val data: DataFrame = spark.read.table("law.law_visit_log_all")
    val distincted_data: Dataset[Row] = data.distinct()
    val handle_page_next_data: DataFrame = distincted_data.withColumn(
      "next_page",
      next_page(col("fullurl"), col("fullurlid")),
      )
    handle_page_next_data.show(5, truncate = false)
    handle_page_next_data.filter("next_page = false").count
  }
}
