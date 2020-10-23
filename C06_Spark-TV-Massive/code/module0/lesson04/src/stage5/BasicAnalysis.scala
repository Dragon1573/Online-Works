package stage5

import org.apache.spark.sql.SparkSession

/**
 * <h1>基础数据探索</h1>
 *
 * @author Dragon1573
 * */
object BasicAnalysis {
  def main(args: Array[String]): Unit = {
    /* 创建 SparkSession 对象并指定日志等级 */
    val spark = SparkSession.builder().appName("基础数据探索").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /* 基础数据探索 */
    Array("media_index_3m", "mediamatch_userevent", "mediamatch_usermsg",
          "mmconsume_billevents", "order_index_v3").foreach(
      tableName => {
        val table = spark.read.table(s"user_profile.$tableName")
        println(s"数据表 `$tableName` 共有${table.count() - table.distinct().count()}行重复记录")
        if (tableName == "media_index_3m") {
          println(s"数据表 `$tableName` 共有${
            table.filter("end_time <= origin_time").count()
          }条结束时间早于开始时间的异常记录")
        }
        println(s"字段 `$tableName.phone_no` 共有${
          table.filter("phone_no = '' OR phone_no = 'null' OR phone_no = 'NULL'").count()
        }行空记录")
        if (tableName == "mediamatch_usermsg") {
          println(s"数据表 `$tableName` 中有${
            table.groupBy("phone_no").count().filter("count > 1").count()
          }个用户分别拥有多条记录")
        }
      })
  }
}
