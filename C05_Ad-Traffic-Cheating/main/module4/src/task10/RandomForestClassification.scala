package task10

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.sql.SparkSession
import task8.DecisionTreeClassification.evaluate
import task8.DecisionTreeClassification.getMachineLearningPipeline
import task8.DecisionTreeClassification.getStandardData

/**
 * <h1>随机森林模型</h1>
 *
 * @author Dragon1573
 * */
object RandomForestClassification {
  def main(args: Array[String]): Unit = {
    // 连接到 Spark Session
    val spark = SparkSession.builder().master("spark://master:7077").appName("Random Forest").
                            config("hive.metastore.uris", "thrift://master:9083").enableHiveSupport().
                            getOrCreate()

    // 加载数据集并标准化
    val standardData = getStandardData(spark.read.table("advertisement.case_data_sample_model"))

    // 数据集划分
    val Array(train, test) = standardData.randomSplit(Array(0.8, 0.2))

    /* 构建并训练随机森林分类器 */
    val randomForest = new RandomForestClassifier().setLabelCol("indexed_label").setFeaturesCol("indexed_features").
                                                   setPredictionCol("prediction").setNumTrees(10)
    val model = getMachineLearningPipeline(randomForest, standardData, train)

    // 将模型写入 HDFS
    model.write.overwrite().save("/user/root/C05/random-forest-model.sml")

    /* 模型预测 */
    val predictions = model.transform(test)
    predictions.show(false)

    // 模型评估
    evaluate(predictions)
  }
}
