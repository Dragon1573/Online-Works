package chapter4.part2

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * <h1>过滤得到法律咨询类别数据</h1>
 */
object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .appName("过滤得到法律咨询类别数据")
                                          .getOrCreate()
    // 加载基础探索后数据
    val data: DataFrame = spark.sql("SELECT * FROM law.law_visit_log_all_cleaned")
    // 获取并保存法律咨询类别数据
    data.filter("fullurlid = '101003'").write.
        mode("overwrite").saveAsTable("law.law_visit_log_all_cleaned_101003")
  }
}
