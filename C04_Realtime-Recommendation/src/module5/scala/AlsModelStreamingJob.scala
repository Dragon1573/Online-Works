import java.text.SimpleDateFormat
import java.util.Date

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/**
 * <h1>ALS 模型实时推荐任务</h1>
 *
 * @author Dragon1573
 */
object AlsModelStreamingJob {
  def main(args: Array[String]): Unit = {
    /* 加载 ALS 模型 */
    val model = ALSModel.read.load("hdfs://master:8020/user/root/C04/ALS_model.sml")

    /* Spark 相关对象 */
    val sparkConf = new SparkConf().setMaster("spark://master:7077").setAppName("ALS实时推荐")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(1))

    /* Kafka 相关配置 */
    val kafkaConfigs = Map[String, Object](
      "bootstrap.servers" -> "slave1:9092,slave2:9092,slave3:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "Streaming Recommendation"
      )
    val topics = Array("shop")

    /* 获取 Kafka 数据源 */
    val source = KafkaUtils.createDirectStream(
      streamingContext, PreferConsistent,
      Subscribe[String, String](topics, kafkaConfigs)
      )

    /* 实时流处理 */
    source.map(_.value).map(_.split(",")). // 获取 Kafka 消息中的内容，并切割为列
          filter(row => row.nonEmpty && row.length == 11). // 过滤有效消息
          map(row => ((row(8).toInt, row(4).toInt), 1.0)). // 转换为记录数据集
          reduceByKeyAndWindow((rate1: Double, rate2: Double) => rate1 + rate2, Minutes(5), Seconds(5)). // 键值对分组聚合
          map(row => (row._1._1, row._1._2, row._2)). // 映射为数据集
          foreachRDD(
            /* 对实时流中划分的每一个 RDD 进行处理*/
            rdd => {
              // 获取当前时间
              val time = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date)

              // 按要求转换为 DataFrame
              val dataset = rdd.toDF("user", "item", "rating")
              // 为实时流中的每个用户推荐5个商品，并写入
              model.recommendForUserSubset(dataset, 5).
                   write.mode(SaveMode.Overwrite).
                   save(s"hdfs://master:8020/user/root/C04/recommendation/$time")

              /* 评估推荐的 RMSE 值 */
              val rmse = new RegressionEvaluator().setMetricName("rmse").
                                                  setLabelCol("rating").
                                                  setPredictionCol("predict").
                                                  evaluate(model.transform(dataset))
              println(
                s"""时间戳：$time\tRMSE：$rmse""")
            })

    /* 启动实时流，并等待任务结束 */
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
