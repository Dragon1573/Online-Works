package chapter4.part4.stage2

/**
 * <h1>ALS模型评估</h1>
 *
 * @author Dragon1573
 * */
object ModelEvaluating {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.ml.evaluation.RegressionEvaluator
    import org.apache.spark.ml.recommendation.ALSModel
    import org.apache.spark.sql.{DataFrame, SparkSession}
    val spark: SparkSession = SparkSession.builder().appName("ALS模型评估").getOrCreate()

    // 载入模型
    val model: ALSModel = ALSModel.read.load("./ALS_Model.sml")
    // 设置模型冷启动策略
    model.setColdStartStrategy("drop")
    // 载入测试集数据
    val test: DataFrame = spark.read.table("law.test_dataset")

    // 执行预测
    val predictions: DataFrame = model.transform(test)

    // 构建评估器
    val evaluator: RegressionEvaluator = new RegressionEvaluator().setMetricName("rmse").
                                                                  setLabelCol("rating").
                                                                  setPredictionCol("prediction")
    // 计算RMSE均方根误差
    val rmse: Double = evaluator.evaluate(predictions)
    println(s"模型的均方根误差为$rmse")
  }
}
