package stage4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.min

/**
 * <h1>有效数据探索</h1>
 *
 * @author Dragon1573
 * */
object ValidationExplore {
  def main(args: Array[String]): Unit = {
    /* 创建 SparkSession 对象并指定日志等级 */
    val spark = SparkSession.builder().appName("有效数据探索").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // 读取数据集
    val media_index = spark.read.table("user_profile.media_index_3m")

    /* 获取时长最值 */
    media_index.select(
      // 最长时长（小时）
      max(col("duration")) / (1000 * 60 * 60) as "max_duration(h)",
      // 最短时长（秒）
      min(col("duration")) / 1000 as "min_duration(s)"
      ).show(false)

    /* 机顶盒未关产生的无效数据 */
    val invalidData = media_index.filter("res_type = 0 AND end_time LIKE '%00' AND origin_time LIKE '%00'").count()
    println(s"表 `media_index` 中的无效记录数共有：${invalidData}条")
  }
}
