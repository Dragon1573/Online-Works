import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

/**
 * <h1>Kafka主题批处理模型训练</h1>
 * <p>
 * &emsp;&emsp;在实际生产环境中，推荐模型并非实时训练产生的。由于时间片太短（1分钟），产生的数据量很小（100条记录），不足以构建一个 ALS 模型。因此，我们选择从 Kafka
 * 主题中提取全部数据作为批处理数据集，依托 Spark Batch 直接读取本地磁盘数据，整体训练一个 ALS 模型，长期生效。
 * </p>
 * <p>&emsp;&emsp;在后续的模型推荐中，我们才借助 Spark Streaming 流式读取 Kafka 主题，并使用我们提前训练好的数据进行推荐。</p>
 *
 * @author Dragon1573
 * */
object AlsModelTrainJob {
  def main(args: Array[String]): Unit = {
    /* 创建 Spark Session 对象 */
    val sparkSession = SparkSession.builder().master("spark://master:7077").appName("ALS模型训练").getOrCreate()
    import sparkSession.implicits._
    println("[Debug] Spark Session 已启用！")

    /* 数据预处理 */
    val dataset = sparkSession.read.textFile("hdfs://master:8020/user/root/C04/raw").
                              map(_.split(",")).
                              filter(row => row.nonEmpty && row.length == 11).
                              map(row => (row(8).toInt, row(4).toInt, 1.0)).
                              groupByKey(row => (row._1, row._2)).
                              reduceGroups((row1, row2) => (row1._1, row1._2, row1._3 + row2._3)).
                              map(_._2).
                              toDF("user", "item", "rating")
    println("[Debug] 数据预处理完成！")

    /* 随机数据集划分 */
    val Array(train, test) = dataset.randomSplit(Array(0.8, 0.2), 0x6A24261B)

    /* 模型训练 */
    val model = new ALS().setMaxIter(10).setRank(10).setRegParam(0.001).setUserCol("user").
                         setItemCol("item").setRatingCol("rating").setPredictionCol("predict").
                         setColdStartStrategy("drop").fit(train)
    println("[Debug] 模型训练完成！")

    /* 存储模型 */
    model.write.overwrite().save("hdfs://master:8020/user/root/C04/ALS_model.sml")
    println("[Debug] 模型已写入 HDFS ！")

    /* 存储测试集 */
    test.write.mode(SaveMode.Overwrite).save("hdfs://master:8020/user/root/C04/test_dataset.csv")
  }
}
