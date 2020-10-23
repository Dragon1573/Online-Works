package lesson6

import lesson5.OrderIndexPreprocess.commonCond
import lesson5.OrderIndexPreprocess.getConnection
import org.apache.spark.sql.SaveMode.Overwrite

/**
 * <h1>数据预处理 - 用户账单信息表</h1>
 *
 * @author Dragon1573
 * */
object BillEventPreprocess {
  def main(args: Array[String]): Unit = {
    val spark = getConnection("数据预处理 - 用户账单信息表")
    spark.read.table("user_profile.mmconsume_billevents").distinct().filter(commonCond).
         write.mode(Overwrite).saveAsTable("user_profile.bill_event_cleaned")
  }
}
