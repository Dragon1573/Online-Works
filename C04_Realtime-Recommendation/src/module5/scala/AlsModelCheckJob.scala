import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.SparkSession

/**
 * <h1>批处理 ALS 模型评估</h1>
 *
 * @author Dragon1573
 * */
object AlsModelCheckJob {
  def main(args: Array[String]): Unit = {
    /* 创建 Spark Session 对象 */
    val sparkSession = SparkSession.builder().master("spark://master:7077").appName("ALS模型评估").getOrCreate()
    import sparkSession.implicits._
    println("[Debug] Spark Session 已启用！")

    /* 从 HDFS 加载已经训练的模型 */
    val trainedModel = ALSModel.read.load("hdfs://master:8020/user/root/C04/ALS_model.sml")
    println("[Debug] ALS 模型加载成功！")

    /* 从 HDFS 加载训练集 */
    val dataset = sparkSession.read.load("hdfs://master:8020/user/root/C04/test_dataset.csv").
                              map(row => (row.getInt(0), row.getInt(1), row.getDouble(2))).
                              toDF("user", "item", "rating")
    println("[Debug] 训练集加载成功！")

    /* 模型预测 */
    val predicted = trainedModel.transform(dataset)
    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").
                                             setPredictionCol("predict").evaluate(predicted)
    println(s"[Debug] 模型的RMSE值为：$evaluator")
  }
}
