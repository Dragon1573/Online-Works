package process

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import sink.MysqlSink

/**
 * <h1>每日门店销售额实时统计</h1>
 *
 * @author Dragon1573
 * */
object StoreSaleJob {
  def main(args: Array[String]): Unit = {
    // 创建 Flink 处理环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 配置 Kafka 数据源
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "slave1:9092,slave2:9092,slave3:9092")
    properties.setProperty("group.id", "store_sale")
    val kafkaSource: DataStream[String] =
      environment.addSource(new FlinkKafkaConsumer[String]("shop", new SimpleStringSchema(), properties))

    // 数据处理，获得并写入 MySQL Sink
    kafkaSource.map(_.split(",")).filter(x => x.nonEmpty && x.length == 11 && x(7).contains("buy")).
               map(x => ((x(x.length - 1), x(6)), x(5).toDouble)).keyBy(0).timeWindow(Time.minutes(1)).sum(1).
               map(x => poso.StoreSaleRecord(x._1._1, x._1._2, x._2)).addSink(new MysqlSink)

    // 执行任务
    environment.execute("每日门店销售额")
  }
}
