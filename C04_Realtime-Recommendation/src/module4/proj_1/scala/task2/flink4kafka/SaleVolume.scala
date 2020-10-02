package task2.flink4kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import task1.mysql_sink.SaleMysqlSink
import task1.poso.CountedObject

/**
 * <h1>每日销售额实时流处理</h1>
 *
 * @author Dragon1573
 * */
object SaleVolume {
  def main(args: Array[String]): Unit = {
    // 创建 Flink 实时流执行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    // 配置 Kafka 集群参数
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "slave1:9092,slave2:9092,slave3:9092")
    properties.setProperty("group.id", "sale_query")

    // 创建数据源
    val flinkStream =
      environment.addSource(new FlinkKafkaConsumer[String]("shop", new SimpleStringSchema(), properties))

    // 数据处理
    val dataset = flinkStream.map(_.split(",")).filter((value: Array[String]) => value.nonEmpty && value.length == 11).
                             map(value => {
                               if (value(7).contains("buy"))
                                 (value(value.length - 1), (value(5).toDouble, 1))
                               else
                                 (value(value.length - 1), (0.0, 1))
                             }).keyBy(0).timeWindow(Time.minutes(1)).
                             reduce((value1, value2) => (value1._1, (value1._2._1 + value2._2._1, value2._2._2)))
    dataset.map(value => CountedObject(value._1, value._2._1)).addSink(new SaleMysqlSink("sale_volume"))
    dataset.map(value => CountedObject(value._1, value._2._2)).addSink(new SaleMysqlSink("visit_count_daily"))

    environment.execute("每日实时销售额和访问量")
  }
}
