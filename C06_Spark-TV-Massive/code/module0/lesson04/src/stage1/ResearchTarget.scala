package stage1

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

/**
 * <h1>需求一：主要研究对象探索</h1>
 *
 * @author Dragon1573
 * */
object ResearchTarget {
  def main(args: Array[String]): Unit = {
    showResults("主要研究对象探索", "owner_name", "owner_code")
  }

  /**
   * <h2>展示指定表和列的不重复记录并对其进行计数</h2>
   * */
  def analysis(table: DataFrame, columnName: String): Unit = {
    table.select(columnName).distinct().show(false)
    table.groupBy(columnName).count().show()
  }

  /**
   * <h2>获取 SparkSession 对象</h2>
   * */
  def loadDatabase(appName: String): Map[String, DataFrame] = {
    val spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    /* 读取数据库，此处使用映射表以整理数据库中的所有表 */
    Map("user_messages" -> spark.read.table("user_profile.mediamatch_usermsg"),
        "bill_events" -> spark.read.table("user_profile.mmconsume_billevents"),
        "user_events" -> spark.read.table("user_profile.mediamatch_userevent"),
        "order_index" -> spark.read.table("user_profile.order_index_v3"),
        "media_index" -> spark.read.table("user_profile.media_index_3m"))
  }

  /**
   * <h2>展示探索结果</h2>
   * */
  def showResults(appName: String, columnName: String*): Unit = {
    val database = loadDatabase(appName)
    Array("user_messages", "bill_events", "user_events", "order_index", "media_index").foreach(table => {
      println(s"数据表 `$table`: ")
      columnName.foreach(analysis(database(table), _))
    })
  }
}
