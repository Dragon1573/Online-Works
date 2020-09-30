package chapter4.part4.stage3

/**
 * <h1>ALS模型调优</h1>
 *
 * @author Dragon1573
 * */
object ModelOptimizing {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.{DataFrame, SparkSession}
    val spark: SparkSession = SparkSession.builder().appName("ALS模型调优").getOrCreate()

    // 设计超参数网格
    val parameterGrid: (List[Int], List[Int], List[Double], List[Boolean]) =
      (List(8, 10), List(5, 10), List(0.01, 0.03), List(true, false))
    var bestParams: (Int, Int, Double, Boolean) = null
    var minErr: Double = 10.0

    // 载入数据集
    val trainData: DataFrame = spark.read.table("law.law_visit_log_all_cleaned_101003_als_train")
    // 划分数据集
    val Array(train: DataFrame, test: DataFrame) = trainData.randomSplit(Array(0.8, 0.2))

    // 遍历超参数网格
    for (rank <- parameterGrid._1;
         iteration <- parameterGrid._2;
         regression <- parameterGrid._3;
         implicitPref <- parameterGrid._4) {
      // 模型训练
      import org.apache.spark.ml.evaluation.RegressionEvaluator
      import org.apache.spark.ml.recommendation.{ALS, ALSModel}
      val als: ALS = new ALS().setMaxIter(iteration).setRegParam(regression).
                              setRank(rank).setImplicitPrefs(implicitPref).
                              setUserCol("uid").setItemCol("pid").setRatingCol("rating")
      val model: ALSModel = als.fit(train)

      // 模型预测
      model.setColdStartStrategy("drop")
      val predictions: DataFrame = model.transform(test)
      val evaluator: RegressionEvaluator =
        new RegressionEvaluator().setMetricName("rmse").
                                 setLabelCol("rating").
                                 setPredictionCol("prediction")
      val rmse: Double = evaluator.evaluate(predictions)
      println(
        s"""当前模型超参数：
           |    矩阵的秩：$rank
           |    迭代次数：$iteration
           |    正则化系数：$regression
           |    隐式标签：$implicitPref
           |RMSE均方根误差值为$rmse
           |""".stripMargin)
      if (minErr > rmse) {
        minErr = rmse
        bestParams = (rank, iteration, regression, implicitPref)
      }
    }

    println(
      s"""最佳模型为：
         |    矩阵的秩：${bestParams._1}
         |    迭代次数：${bestParams._2}
         |    正则化系数：${bestParams._3}
         |    隐藏标签${bestParams._4}
         |模型的RMSE均方根误差为：$minErr""".stripMargin)
  }
}
