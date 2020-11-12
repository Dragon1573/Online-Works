package lesson12

import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.row_number

/**
 * <h1>销售品名称画像描绘</h1>
 *
 * @author Dragon1573
 * */
object ItemName {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("画像描绘：销售品名称").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // 读取数据集
    val source = spark.read.table("user_profile.order_index_cleaned").filter("cost >= 0")
                      .filter("offername NOT LIKE '%空包%'")

    /* 按照要求构建标签 */
    val television = source.filter("sm_name NOT LIKE '珠江宽频'")
    val broadband = source.filter("sm_name LIKE '珠江宽频'")
    val tvMajorItems = television.filter("mode_time = 'Y' AND offertype = 0 AND prodstatus = 'YY'").
                                 select(
                                   col("phone_no"), col("offername"),
                                   row_number().over(partitionBy("phone_no").orderBy(desc("optdate"))) as
                                     "optdate_index"
                                   ).filter("optdate_index = 1").
                                 selectExpr("phone_no", "offername AS label")
    val tvMinorItems = television.filter("mode_time = 'Y' AND offertype = 1 AND prodstatus = 'YY'").
                                 selectExpr("phone_no", "offername AS label")
    val broadbandItems = broadband.select(
      col("phone_no"), col("offername"),
      row_number().over(partitionBy("phone_no").orderBy(desc("optdate"))) as "optdate_index"
      ).filter("optdate_index = 1").selectExpr("phone_no", "offername AS label")
    val result = tvMajorItems.union(tvMinorItems).union(broadbandItems).distinct().
                             selectExpr("phone_no", "label", "'销售品名称' AS class")

    // 将数据表写入 Hive
    result.write.mode(Append).saveAsTable("user_profile.user_labels")
  }
}
