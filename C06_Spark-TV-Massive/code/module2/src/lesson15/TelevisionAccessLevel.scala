package lesson15

import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.functions.udf

/**
 * <h1>电视入网程度画像描绘</h1>
 *
 * @author Dragon1573
 * */
object TelevisionAccessLevel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("画像描绘：电视入网程度").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // 读取数据集
    val source = spark.read.table("user_profile.user_message_cleaned")

    /* 数据处理 */
    // 获取最早注册时间
    val earliestDate = source.filter("sm_name LIKE '%互动%' OR sm_name LIKE '%甜果%' OR sm_name LIKE '%数字%'").
                             select(
                               col("phone_no"), col("open_time"),
                               row_number().over(partitionBy("phone_no").orderBy("open_time")) as "index"
                               ).filter("index = 1").selectExpr("phone_no", "open_time AS index")
    // 注册为 Spark SQL 临时视图
    earliestDate.createOrReplaceTempView("earliest_date")
    // 执行 Spark SQL 查询语句，计算年份间隔
    val duration = spark.sql(
      """
        | SELECT
        |     phone_no,
        |     months_between(from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss'), index) / 12 AS months
        | FROM earliest_date""".stripMargin)
    // 自定义 Spark SQL 列自定义函数
    val getLabel = udf((months: Double) => {
      if (months <= 3) "新用户"
      else if (3 < months && months <= 6) "中等用户"
      else if (6 < months) "老用户"
      else "NULL"
    })
    // 获取结果
    val result = duration.withColumn("label", getLabel(col("months"))).
                         selectExpr("phone_no", "label", "'电视入网程度' AS class")

    // 结果写入 Hive
    result.write.mode(Append).saveAsTable("user_profile.user_labels")
  }
}
