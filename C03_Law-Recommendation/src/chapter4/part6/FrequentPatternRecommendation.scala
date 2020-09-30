package chapter4.part6

/**
 * <h1>FP关联规则模型的构建、评估与调优</h1>
 * <p>集群的 Spark 堆栈容量和 JVM 堆栈容量不够，跑 <code>FPGrowth</code> 会出问题。</p>
 *
 * @author Dragon1573
 */
object FrequentPatternRecommendation {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.fpm.{FPGrowth, FPGrowthModel}
    import org.apache.spark.sql.{DataFrame, SparkSession}
    val spark: SparkSession = SparkSession.builder().appName("FP模型全流程").getOrCreate()

    // 加载数据
    val data: DataFrame = spark.read.table("law.law_visit_log_all_cleaned_101003_fp_dataset")
    // 随机划分数据集
    val Array(train: DataFrame, test: DataFrame) = data.randomSplit(Array(0.8, 0.2))

    // 构建模型
    val fpGrowth: FPGrowth = new FPGrowth().setItemsCol("url_sets").
                                           setMinSupport(0.00005).
                                           setMinConfidence(0.003)
    // 训练模型
    val model: FPGrowthModel = fpGrowth.fit(train)
    // 展示模型中的频繁项集
    model.freqItemsets.show(false)
    // 展示模型中的关联规则
    model.associationRules.show()

    // 按照频繁项集进行推荐
    model.transform(test).show()
  }
}
