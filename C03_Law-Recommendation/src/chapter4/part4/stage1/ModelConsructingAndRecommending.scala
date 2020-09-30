package chapter4.part4.stage1

/**
 * <h1>ALS模型构建与推荐</h1>
 *
 * @author Dragon1573
 */
object ModelConsructingAndRecommending {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.recommendation.{ALS, ALSModel}
    import org.apache.spark.sql.functions.{col, explode}
    import org.apache.spark.sql.{DataFrame, SparkSession}
    val spark: SparkSession = SparkSession.builder().
                                          appName("ALS模型构建与推荐").
                                          getOrCreate()

    // 加载数据
    val trainData: DataFrame = spark.
                               sql("SELECT * FROM law.law_visit_log_all_cleaned_101003_als_train")
    // 划分数据集
    val Array(train, test) = trainData.randomSplit(Array(0.8, 0.2))

    // 构建ALS推荐模型（最大迭代次数、矩阵的秩、正则化参数）
    val als: ALS = new ALS().setMaxIter(5).setRegParam(0.01).setRank(10).
                            setUserCol("uid").setItemCol("pid").setRatingCol("rating")
    val model: ALSModel = als.fit(train)

    // 为所有用户推荐10个最佳链接
    val userRecs: DataFrame = model.recommendForAllUsers(10)
    userRecs.show(5, 100)

    // 拼接用户推荐列表
    val userRecommend: DataFrame =
      userRecs.select(col("uid"), explode(col("recommendations")) as "recommendations").
              select("uid", "recommendations.pid", "recommendations.rating")
    userRecommend.show(5)
    val userMeta: DataFrame = spark.read.table("law.law_visit_log_all_userMeta")
    val urlMeta: DataFrame = spark.read.table("law.law_visit_log_all_urlMeta")
    val result: DataFrame =
      userRecommend.join(userMeta, userRecommend("uid") === userMeta("uid"), "leftouter").
                   join(urlMeta, userRecommend("pid") === urlMeta("pid"), "leftouter").
                   select("userid", "fullurl", "rating")
    result.show(false)

    // 将推荐结果写入Hive
    result.write.mode("overwrite").saveAsTable("law.als_recommends")
    // 将模型保存至HDFS
    model.write.overwrite().save("./ALS_Model.sml")
    // 导出测试集
    test.write.mode("overwrite").saveAsTable("law.test_dataset")
  }
}
