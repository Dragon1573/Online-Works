package lesson13

import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.row_number

/**
 * <h1>宽带产品带宽画像描绘</h1>
 *
 * @author Dragon1573
 * */
object BroadbandItemBandwidth {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("画像描绘：宽带产品带宽").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // 读取数据集
    val source = spark.read.table("user_profile.order_index_cleaned")

    // 按要求贴标签
    val result = source.filter("sm_name LIKE '%珠江宽频%'").
                       select(
                         col("phone_no"), col("prodname"),
                         row_number().over(partitionBy("phone_no").orderBy(desc("optdate"))) as "index"
                         ).filter("index = 1").selectExpr("phone_no", "prodname AS label", "'宽带产品带宽' AS class")

    // 写入 Hive
    result.write.mode(Append).saveAsTable("user_profile.user_labels")
  }
}
