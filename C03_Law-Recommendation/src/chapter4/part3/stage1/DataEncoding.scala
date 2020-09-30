package chapter4.part3.stage1

/**
 * <h1>数据编码</h1>
 */
object DataEncoding {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.{DataFrame, SparkSession}
    val spark: SparkSession = SparkSession.builder().appName("数据编码").getOrCreate()

    // 载入数据
    val data: DataFrame = spark.
                          sql("SELECT * FROM law.law_visit_log_all_cleaned_101003").
                          select("userid", "fullurl", "timestamps")
    // 创建记录封装
    case class User_ID(userid: String, uid: Int)
    // 创建用户映射表
    import spark.implicits._
    val allUsers: DataFrame = data.select("userid").distinct().orderBy("userid").rdd.
                                  map(_.getString(0)).zipWithIndex().
                                  map((x: (String, Long)) => User_ID(x._1, x._2.toInt)).toDF()

    // 创建记录封装
    case class Item_ID(fullurl: String, pid: Int)
    // 创建链接映射表
    val allItems: DataFrame = data.select("fullurl").distinct().orderBy("fullurl").rdd.
                                  map(_.getString(0)).zipWithIndex().
                                  map(x => Item_ID(x._1, x._2.toInt)).toDF()

    // 将两个数据表拼接在一起
    val encoded_data: DataFrame =
      data.join(allUsers, data("userid") === allUsers("userid")).
          drop(allUsers("userid")).
          join(allItems, data("fullurl") === allItems("fullurl"), "leftouter").
          drop(allItems("fullurl"))
    encoded_data.show(5)

    // 将数据写入Hive
    encoded_data.write.mode("overwrite").
                saveAsTable("law.law_visit_log_all_cleaned_101003_encoded")
    allUsers.write.mode("overwrite").saveAsTable("law.law_visit_log_all_userMeta")
    allItems.write.mode("overwrite").saveAsTable("law.law_visit_log_all_urlMeta")
  }
}
