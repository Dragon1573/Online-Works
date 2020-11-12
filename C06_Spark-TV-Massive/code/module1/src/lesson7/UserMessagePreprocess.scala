package lesson7

import lesson5.OrderIndexPreprocess.commonCond
import lesson5.OrderIndexPreprocess.getConnection
import org.apache.spark.sql.SaveMode.Overwrite

/**
 * <h1>数据预处理 - 用户信息表</h1>
 *
 * @author Dragon1573
 * */
object UserMessagePreprocess {
  // 用户状态过滤
  val runNameCond = "run_name = '正常' OR run_name = '主动暂停' OR run_name = '主动销号' OR run_name = '欠费暂停'"

  def main(args: Array[String]): Unit = {
    val spark = getConnection("数据预处理 - 用户信息表")
    spark.read.table("user_profile.mediamatch_usermsg").distinct().filter(commonCond).filter(runNameCond).
         write.mode(Overwrite).saveAsTable("user_profile.user_message_cleaned")
  }
}
