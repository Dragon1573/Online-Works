package lesson11

import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * <h1>宽带消费水平画像描绘</h1>
 *
 * @author Dragon1573
 * */
object BroadbandConsumeLevel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("画像描绘：宽带消费水平").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // 载入数据集
    val sourceTable = spark.read.table("user_profile.bill_event_cleaned")

    /* 获取最大日期前3个月的数据 */
    sourceTable.filter("sm_name LIKE '%珠江宽频%'").selectExpr("max(year_month) AS max_date").
               createOrReplaceTempView("max_date_view")
    val beginDate = spark.sql("SELECT add_months(max_date, -3) AS begin_date from max_date_view")
    val windowedData = sourceTable.crossJoin(beginDate).filter("year_month > begin_date")

    // 统计月均账单
    val averageBroadband = windowedData.groupBy("phone_no").
                                       agg(sum(col("should_pay") - col("favour_fee")) / 3 as "avg_broadband_bill")

    // 贴标签
    val getLabel = udf((value: Double) => {
      if (20 >= value) "宽带低消费"
      else if (20 < value && 50 >= value) "宽带低消费"
      else if (50 < value) "宽带高消费"
      else "NULL"
    })
    val result = averageBroadband.withColumn("label", getLabel(col("avg_broadband_bill"))).
                                 selectExpr("phone_no", "label", "'宽带消费水平' AS class")

    // 将数据表写入 Hive
    result.write.mode(Append).saveAsTable("user_profile.user_labels")
  }
}
