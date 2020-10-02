package process

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import poso.StringDoublePair

/**
 * <h1>商品销量实时统计，筛选十大热门商品</h1>
 *
 * @author Dragon1573
 * */
object ItemVolume10Best {
  def main(args: Array[String]): Unit = {
    // 创建 Flink 环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    // 配置 Kafka 数据源
    val kafka = new Properties()
    kafka.setProperty("bootstrap.servers", "slave1:9092,slave2:9092,slave3:9092")
    kafka.setProperty("group.id", "ten_item_best")
    val source = environment.addSource(new FlinkKafkaConsumer[String]("shop", new SimpleStringSchema(), kafka))

    // Flink 实时流处理
    source.map(_.split(",")).filter(x => x.nonEmpty && x.length == 11 && x(7).contains("buy")).
          map(x => StringDoublePair(x(4), 1.0)).keyBy(_.string).timeWindow(Time.minutes(1)).
          process(new ProcessWindowFunction[StringDoublePair, String, String, TimeWindow] {
            override def process(key: String, context: Context,
                                 elements: Iterable[StringDoublePair],
                                 out: Collector[String]): Unit = {
              val getSum = elements.groupBy(_.string).map(x => (x._1, x._2.map(_.double).sum))
              getSum.toList.sortBy(_._2).reverse.take(10).
                    foreach(x => out.collect(s"${System.currentTimeMillis().toString}:${x._1}:${x._2}"))
            }
          }).print("Console")

    // 执行任务
    environment.execute("十佳商品实时排行榜")
  }
}
