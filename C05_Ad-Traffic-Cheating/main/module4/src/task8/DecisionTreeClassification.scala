package task8

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

/**
 * <h1>决策树分类模型</h1>
 *
 * @author Dragon1573
 * */
object DecisionTreeClassification {
  /* 连接到 Spark Session */
  val spark = SparkSession.builder().master("spark://master:7077").appName("Decision Tree").
                          config("hive.metastore.uris", "thrift://master:9083").enableHiveSupport().
                          getOrCreate()

  // 预测数据集
  var predictions: DataFrame = _

  /**
   * <h2>数据标准化</h2>
   * */
  def getStandardData(data: DataFrame): DataFrame = {
    val vectorAssembler = new VectorAssembler().setInputCols(Array("N1", "N2", "N3", "N4")).setOutputCol("features")
    val minMaxScaler = new MinMaxScaler().setInputCol("features").setOutputCol("scaled_features")
    new Pipeline().setStages(Array(vectorAssembler, minMaxScaler)).fit(data).transform(data)
  }

  /**
   * <h2>构建 Spark 分类模型</h2>
   * */
  def getMachineLearningPipeline(algorithm: PipelineStage,
                                 standardData: DataFrame,
                                 train: DataFrame): PipelineModel = {
    val labelIndexerModel = new StringIndexer().setInputCol("label").setOutputCol("indexed_label").fit(standardData)
    val featureIndexerModel = new VectorIndexer().setInputCol("scaled_features").setOutputCol("indexed_features").
                                                 setMaxCategories(4).fit(standardData)
    val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predicted_label").
                                            setLabels(labelIndexerModel.labels)
    new Pipeline().setStages(Array(labelIndexerModel, featureIndexerModel, algorithm, labelConverter)).fit(train)
  }

  /**
   * <h2>构建模型并执行预测</h2>
   * */
  def buildAndPredict(): DataFrame = {
    /* 特征数据标准化 */
    val standardData = getStandardData(spark.read.table("advertisement.case_data_sample_model"))

    // 数据集划分
    val Array(train, test) = standardData.randomSplit(Array(0.8, 0.2))

    /* 构建决策树分类管道 */
    val decisionTree = new DecisionTreeClassifier().setLabelCol("indexed_label").setFeaturesCol("indexed_features").
                                                   setPredictionCol("prediction")
    val pipelineModel = getMachineLearningPipeline(decisionTree, standardData, train)

    // 将训练好的模型写入 HDFS
    pipelineModel.write.overwrite().save("/user/root/C05/decision_tree_model.sml")

    /* 执行预测分类 */
    predictions = pipelineModel.transform(test)
    predictions.show(false)
    predictions
  }

  /**
   * <h2>对模型进行评估</h2>
   * */
  def evaluate(predictions: DataFrame): Unit = {
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexed_label").
                                                           setPredictionCol("prediction").
                                                           setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    val metrics = new MulticlassMetrics(
      predictions.select("prediction", "indexed_label").
                 rdd.map(row => (row.getDouble(0), row.getDouble(1)))
      )
    println(
      s"""Decision Tree Model:
         |    Accuracy = $accuracy
         |    Test Error = ${1.0 - accuracy}
         |    Confusion Matrix = [
         |${metrics.confusionMatrix}
         |    ]
         |    F1 Value = ${metrics.weightedFMeasure}
         |    Precision Rate = ${metrics.weightedPrecision}
         |    Recall Rate = ${metrics.weightedRecall}""".stripMargin
      )
  }

  def main(args: Array[String]): Unit = {
    evaluate(buildAndPredict())
  }
}
