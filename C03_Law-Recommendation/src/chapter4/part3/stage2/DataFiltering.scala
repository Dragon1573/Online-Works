package chapter4.part3.stage2

/**
 * <h1>过滤重复及单次访问用户</h1>
 *
 * @author Dragon1573
 */
object DataFiltering {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.{DataFrame, SparkSession}
    val spark: SparkSession = SparkSession.builder().appName("过滤重复及单次访问用户").getOrCreate()

    // 用户访问分布情况分析
    val data: DataFrame = spark.read.table("law.law_visit_log_all_cleaned_101003_encoded")
    val datacount: Long = data.count()
    val userNum: Long = data.select("userid").distinct().count()
    val clickCount: DataFrame =
      data.groupBy("userid").
          agg(count("userid") as "user_clicks").
          groupBy("user_clicks").
          agg(count("userid") as "user_clicks_count",
              count("userid") * 100.0 / userNum as "user_per",
              count("userid") * 100.0 * col("user_clicks") / datacount as "user_click_per").
          orderBy(desc("user_click_per"))
    clickCount.show(false)

    // 过滤单次点击用户并去重
    val data_distinct: DataFrame = data.select(col("uid"), col("pid"), col("timestamps")).distinct
    val user_gt_one: DataFrame = data_distinct.groupBy("uid").
                                              agg(collect_list("pid") as "url_list").
                                              filter(size(col("url_list")) > 1).
                                              select(col("uid"))
    val get_one: DataFrame = data_distinct.join(user_gt_one, "uid")
    get_one.write.mode("overwrite").
           saveAsTable("law.law_visit_log_all_cleaned_101003_endcoded_gt_one")
  }
}
