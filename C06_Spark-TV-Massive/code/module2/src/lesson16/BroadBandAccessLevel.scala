package lesson16

import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.functions.udf

/**
 * <h1>宽带入网程度画像描绘</h1>
 *
 * @author Dragon1573
 * */
object BroadBandAccessLevel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("画像描绘：宽带入网程度").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // 读取数据
    val source = spark.read.table("user_profile.user_message_cleaned")

    /* 数据处理 */
    // 提取宽带数据
    val broadBand = source.filter("sm_name LIKE '%珠江宽频%' AND sm_code = 'b0' AND force LIKE '%宽带生效%'").
                          select(col("phone_no"), col("open_time"), row_number().over(
                            partitionBy("phone_no").orderBy("open_time")
                            ) as "index").filter("index = 1").select("phone_no", "open_time")
    // 注册临时视图
    broadBand.createOrReplaceTempView("broadband_view")
    // 使用 Spark SQL 计算用户入网时长
    val duration = spark.sql(
      """ SELECT
        |     phone_no,
        |     months_between(from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss'), open_time) / 12 as duration
        | FROM broadband_view""".stripMargin
      )

    // DataFrame 列级自定义函数
    def getLabel = udf((years: Double) => {
      if (years <= 2) "新用户"
      else if (2 < years && years <= 4) "中等用户"
      else if (4 < years) "老用户"
      else "NULL"
    })

    // 贴标签
    val result = duration.withColumn("label", getLabel(col("duration"))).
                         selectExpr("phone_no", "label", "'宽带入网程度' AS class")
    // 写入 Hive
    result.write.mode(Append).saveAsTable("user_profile.user_labels")
  }
}
