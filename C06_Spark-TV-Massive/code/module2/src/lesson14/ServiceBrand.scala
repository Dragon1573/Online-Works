package lesson14

import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.SparkSession

/**
 * <h1>业务品牌画像描绘</h1>
 *
 * @author Dragon1573
 * */
object ServiceBrand {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("画像描绘：业务品牌").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // 读取数据集
    val source = spark.read.table("user_profile.user_message_cleaned")

    // 数据过滤，添加标签
    val result = source.filter("sm_name != 'a0' AND sm_name != 'b1'").
                       selectExpr("phone_no", "sm_name AS label", "'业务品牌' AS class")

    // 写入 Hive
    result.write.mode(Append).saveAsTable("user_profile.user_labels")
  }
}
