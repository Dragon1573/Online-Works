package lesson5

import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession

/**
 * <h1>数据预处理 - 用户订单表</h1>
 *
 * @author Dragon1573
 * */
object OrderIndexPreprocess {
  // 用户对象过滤
  val commonCond = "owner_name != 'EA级' AND owner_name != 'EB级' AND owner_name != 'EC级' AND owner_name != 'ED级'" +
    " AND owner_name != 'EE级' AND owner_code != '02' AND owner_code != '09' AND owner_code != '10'"

  /** <h2>获取 SparkSession 连接</h2> */
  def getConnection(appName: String): SparkSession = {
    val spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark
  }

  def main(args: Array[String]): Unit = {
    val spark = getConnection("数据预处理 - 用户订单表")
    spark.read.table("user_profile.order_index_v3").distinct().filter(commonCond).
         write.mode(Overwrite).saveAsTable("user_profile.order_index_cleaned")
  }
}
