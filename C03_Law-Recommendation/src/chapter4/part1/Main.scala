package chapter4.part1

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, substring, udf}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * <h1>异常数据处理</h1>
 */
object Main {
  /**
   * <h2>判断是否为小数值页码</h2>
   * <p>只有2位以下的数值才是页码，其他的算是商品代码。</p>
   *
   * @param num 编号字符串
   * @return 布尔判断结果
   */
  def small_int(num: String): Boolean = {
    if (StringUtils.isNumeric(num) && num.length < 3) true else false
  }

  /**
   * <h2>判断是否为多页链接</h2>
   *
   * @param page     完整链接
   * @param url_type 网址类型
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
    val spark: SparkSession = SparkSession.builder().appName("异常数据处理").getOrCreate()
    val next_page: UserDefinedFunction =
      udf((page: String, url_type: String) => !is_next_page(page, url_type))

    // 重复异常数据处理，翻页网页统计
    val data: DataFrame = spark.sqlContext.sql("SELECT * FROM law.law_visit_log_all")
    val distincted_data: Dataset[Row] = data.distinct()
    val handle_page_next_data: DataFrame =
      distincted_data.withColumn("next_page", next_page(col("fullurl"), col("fullurlid")))

    //翻页异常数据还原
    val revert_page: UserDefinedFunction = udf((page: String, next_page: Boolean) => {
      if (!next_page) {
        val _split: Int = page.lastIndexOf("_")
        page.substring(0, _split) + ".html"
      } else {
        page
      }
    })
    val handled_page_next_data: DataFrame =
      handle_page_next_data.withColumn("url", revert_page(col("fullurl"), col("next_page")))
    handled_page_next_data.
    filter("next_page = false").select(
      substring(col("fullurl"), 30, 60),
      substring(col("url"), 30, 60)).show(10, truncate = false)

    //被分享网页数据处理
    val revert_share_page: UserDefinedFunction =
      udf((page: String, url_type: String) => {
        if (!"1999001".equals(url_type)) {
          if (page != null && page.contains("?")) {
            page.substring(0, page.indexOf("?"))
          } else {
            page
          }
        } else {
          page
        }
      })
    val handled_share_data: DataFrame = handled_page_next_data.withColumn(
      "url", revert_share_page(col("url"), col("fullurlid")))
    handled_share_data.filter("fullurlid != '1999001'").
                      filter("fullurl LIKE '%?%'").
                      select("fullurl", "url").
                      show(4, truncate = false)

    // 基础探索异常数据处理数据备份
    handled_share_data.write.mode("overwrite").saveAsTable("law.law_visit_log_all_cleaned")

    // 统计处理后的数据集
    handled_share_data.count()
  }
}
