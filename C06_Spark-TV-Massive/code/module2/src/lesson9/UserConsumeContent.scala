package lesson9

import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf

/**
 * <h1>用户消费内容画像描绘</h1>
 *
 * @author Dragon1573
 * */
object UserConsumeContent {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("画像描绘：用户消费内容").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /* 载入清洗后的数据集 */
    val sourceTable = spark.read.table("user_profile.bill_event_cleaned")

    /* 根据要求贴标签 */
    val getLabel = udf((code: String) => {
      if (code == "0J" || code == "0B" || code == "0Y") "直播"
      else if (code == "0X") "应用"
      else if (code == "0T") "付费频道"
      else if (code == "0W" || code == "0L" || code == "0Z" || code == "0K") "宽带"
      else if (code == "0D") "点播"
      else if (code == "0H") "回看"
      else if (code == "0U") "有线电视收费"
      else "NULL"
    })
    val result = sourceTable.select("phone_no", "fee_code").distinct().withColumn("label", getLabel(col("fee_code"))).
                            selectExpr("phone_no", "label", "'消费内容' AS class")

    /* 将结果保存到 Hive 中 */
    result.write.mode(Append).saveAsTable("user_profile.user_labels")
  }
}
