package lesson18

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

/**
 * <h1>SVM 模型训练、评估与调优</h1>
 *
 * @author Dragon1573
 * */
object SvmModelConstruct {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SVM模型：训练、评估与调优").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // 加载模型数据集
    val dataset = spark.read.table("user_profile.svm_dataset")

    /* 构建 Spark ML Pipeline 机器学习流水线 */
    // 电视消费水平索引映射
    val consumeLevelIndexer = new StringIndexer().setInputCol("tv_consume_level").setOutputCol("consume_level_index")
    // 电视入网程度索引映射
    val accessLevelIndexer = new StringIndexer().setInputCol("tv_access_duration").setOutputCol("access_level_index")
    // 特征向量构建器
    val vectorAssembler = new VectorAssembler().setInputCols(
      Array("consume_level_index", "access_level_index", "avg_duration")
      ).setOutputCol("features")
    // LinearSVM 线性支持向量机模型
    val svm = new LinearSVC().setStandardization(true).setFitIntercept(true).setMaxIter(5).
                             setFeaturesCol("features").setLabelCol("final_tag").setPredictionCol("prediction")
    // 将它们整理到流水线中
    val pipeline = new Pipeline().setStages(Array(consumeLevelIndexer, accessLevelIndexer, vectorAssembler, svm))

    // 数据集划分
    val Array(train, test) = dataset.randomSplit(Array(0.8, 0.2), 0x6A24261B)
    println(s"训练集共有${train.count()}行")
    println(s"测试集共有${test.count()}行")

    // 模型训练
    val pipelineModel = pipeline.fit(train)
    // 模型预测
    val prediction = pipelineModel.transform(test)

    /* 模型评估 */
    // 构建评估器
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("final_tag").setPredictionCol("prediction").
                                                           setMetricName("accuracy")
    // 输出评估结果（准确率）
    println(s"模型准确率为：${evaluator.evaluate(prediction)}")
  }
}
