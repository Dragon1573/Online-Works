package lessonX

import lesson5.OrderIndexPreprocess.commonCond
import lesson5.OrderIndexPreprocess.getConnection
import lesson7.UserMessagePreprocess.runNameCond
import org.apache.spark.sql.SaveMode.Overwrite

/**
 * <h1>数据预处理 - 用户状态变更表</h1>
 *
 * @author Dragon1573
 * */
object UserEventPreprocess {
  // 品牌过滤
  val smNameCond = "sm_name = '珠江宽频' OR sm_name = '互动电视' OR sm_name = '甜果电视' OR sm_name = '数字电视'"

  def main(args: Array[String]): Unit = {
    val spark = getConnection("数据预处理 - 用户状态变更表")
    spark.read.table("user_profile.mediamatch_userevent").
         distinct().filter(commonCond).filter(smNameCond).filter(runNameCond).
         write.mode(Overwrite).saveAsTable("user_profile.user_event_cleaned")
  }
}
