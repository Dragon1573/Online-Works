package chapter4.part5

/**
 * <h1>FP关联规则模型的数据准备工作</h1>
 *
 * @author Dragon1573
 */
object ModelPreparation {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.functions.{col, collect_set, size}
    import org.apache.spark.sql.{DataFrame, SparkSession}
    val spark: SparkSession = SparkSession.builder().appName("FP数据准备").getOrCreate()

    // 加载数据
    val data: DataFrame = spark.read.table("law.law_visit_log_all_cleaned_101003")
    // 数据去重，过滤单次访问记录
    val filtered_data: DataFrame = data.select("userid", "fullurl").distinct().groupBy("userid").
                                       agg(collect_set(col("fullurl")) as "url_sets").
                                       filter(size(col("url_sets")) > 1)

    // 展示数据清洗结果
    println("过滤后的数据集拥有" + filtered_data.count() + "行记录")
    filtered_data.show(5, truncate = false)

    // 将数据集转存到Hive
    filtered_data.write.mode("overwrite").saveAsTable("law.law_visit_log_all_cleaned_101003_fp_dataset")
  }
}
