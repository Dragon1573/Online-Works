package chapter4.part3.stage3

/**
 * <h1>评分映射</h1>
 *
 * @author Dragon1573
 */
object ScoreMapping {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.expressions.UserDefinedFunction
    import org.apache.spark.sql.functions.{col, count, lit, udf}
    import org.apache.spark.sql.{DataFrame, SparkSession}
    val spark: SparkSession = SparkSession.builder().appName("评分映射").getOrCreate()

    // 读取数据集
    val data: DataFrame = spark.read.table("law.law_visit_log_all_cleaned_101003_endcoded_gt_one")

    // 建立浏览次数与评分的映射
    def trans_times_2_rate(times: Int): Double = {
      if (times <= 8) times else if (times > 12) 10 else 9
    }

    // 处理数据
    val get_rate: UserDefinedFunction = udf((frequency: Int) => trans_times_2_rate(frequency))
    val get_rate_data: DataFrame = data.groupBy("uid", "pid").
                                       agg(count(lit(1)) as "rating").
                                       withColumn("rating", get_rate(col("rating")))
    get_rate_data.count
    get_rate_data.write.mode("overwrite").
                 saveAsTable("law.law_visit_log_all_cleaned_101003_als_train")
  }
}
