import KafkaLoadMode.BATCH
import KafkaLoadMode.STREAM
import org.apache.log4j.Logger
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

/**
 * <h1>Kafka主题批处理模型训练</h1>
 * <p>
 * &emsp;&emsp;在实际生产环境中，推荐模型并非实时训练产生的。由于时间片太短（1分钟），产生的数据量很小（100条记录），不足以构建一个 ALS
 * 模型。因此，我们选择从 Kafka 主题中提取全部数据作为批处理数据集，依托 Spark Structured Stream 将 Kafka
 * 主题整体作为一个大型数据集训练 ALS 模型，长期生效。
 * </p>
 * <p>&emsp;&emsp;在后续的模型推荐中，我们才借助 Spark Streaming 流式读取 Kafka 主题，并使用我们提前训练好的数据进行推荐。</p>
 *
 * @author Dragon1573
 * */
object AlsModelTrainJob {
  def main(args: Array[String]): Unit = {
    val dataset: DataFrame = dataPreprocess("ALS模型训练", KafkaLoadMode.BATCH)

    /* 模型训练 */
    val Array(train, test) = dataset.randomSplit(Array(0.8, 0.2))
    val model = new ALS().setMaxIter(10).setRank(10).setRegParam(0.001).setColdStartStrategy("drop").
                         setUserCol("user").setItemCol("item").setRatingCol("rating").setPredictionCol("predict").
                         fit(train)

    /* 存储模型 */
    model.write.overwrite().save("hdfs://master:8020/user/root/C04/ALS.sml")
    val logger = Logger.getLogger(AlsModelTrainJob.getClass)
    logger.warn("ALS模型存储完成！")

    /* 存储测试集 */
    test.write.mode(SaveMode.Overwrite).save("hdfs://master:8020/user/root/C04/test.csv")
    logger.warn("测试集存储完成！")
  }

  /** <h2>数据预处理</h2> */
  def dataPreprocess(appName: String, mode: KafkaLoadMode): DataFrame = {
    /* 创建 Spark Session 对象 */
    val spark = SparkSession.builder().appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    /* 数据预处理 */
    var source: DataFrame = mode match {
      case BATCH => spark.read.format("kafka").options(Map(
        "kafka.bootstrap.servers" -> "slave1:9092,slave2:9092,slave3:9092",
        "subscribe" -> "shop"
        )).load()
      case STREAM => spark.readStream.format("kafka").options(Map(
        "kafka.bootstrap.servers" -> "slave1:9092,slave2:9092,slave3:9092",
        "subscribe" -> "shop"
        )).load()
    }
    source.selectExpr("CAST(value AS STRING)").as[String].map(_.split(",")).
          filter(row => row.nonEmpty && row.length == 11).map(row => (row(8).toLong, row(4).toLong, 1)).
          toDF("user", "item", "rating").groupBy("user", "item").sum("rating").
          withColumnRenamed("sum(rating)", "rating")
  }
}
