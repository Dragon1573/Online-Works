package lesson8

import lesson5.OrderIndexPreprocess.commonCond
import lesson5.OrderIndexPreprocess.getConnection
import org.apache.spark.sql.SaveMode.Overwrite

/**
 * <h1>数据预处理 - 用户收视数据表</h1>
 *
 * @author Dragon1573
 * */
object MediaIndexPreprocess {
  def main(args: Array[String]): Unit = {
    val spark = getConnection("数据预处理 - 用户收视数据表")
    spark.read.table("user_profile.media_index_3m").
         distinct().filter(commonCond).
         filter("duration > (4 * 1000) AND duration < (6 * 3600 * 1000)").
         filter("res_type != 0 OR end_time NOT LIKE '%00' OR origin_time NOT LIKE '%00'").
         write.mode(Overwrite).saveAsTable("user_profile.media_index_cleaned")
  }
}
