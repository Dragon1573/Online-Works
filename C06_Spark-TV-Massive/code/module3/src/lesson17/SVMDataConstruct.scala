package lesson17

import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * <h1>SVM 数据集构建</h1>
 *
 * @author Dragon1573
 * */
object SVMDataConstruct {
  /**
   * <h2>自定义 DataFrame 函数</h2>
   * <p>为数据集贴标签，原本是1的标签则保留，其他的统一为0标签</p>
   * */
  def getLabel = udf((rawTag: String) => if (rawTag == "1") 1 else 0)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SVM模型：数据集构建").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // A. 电视消费水平特征
    val tvConsumeLevel = spark.read.table("user_profile.user_labels").filter("class = '电视消费水平'").
                              selectExpr("phone_no", "label AS tv_consume_level")

    // B. 电视入网时长特征
    val tvAccessDuration = spark.read.table("user_profile.user_labels").filter("class = '电视入网程度'").
                                selectExpr("phone_no", "label AS tv_access_duration")

    /* C. 电视依赖度特征 */
    // 读取用户收视数据，过滤得到电视用户数据
    val tvMediaIndex = spark.read.table("user_profile.media_index_cleaned").filter("sm_name NOT LIKE '%珠江宽频%'")
    // 获取距离当前最近的3个月前的日期
    val beginTime = tvMediaIndex.selectExpr("max(end_time) AS latest_time")
                                .selectExpr("add_months(latest_time, -3) AS begin_time")
    // 过滤得到近3个月的数据
    val windowedMediaIndex = tvMediaIndex.crossJoin(beginTime).filter("end_time > begin_time")
    // 统计日均观看时长
    val tvAddictionRate = windowedMediaIndex.groupBy("phone_no").
                                            agg(sum("duration") / (1000 * 60 * 60) / 90 as "avg_duration").
                                            select("phone_no", "avg_duration")

    /* 标签列 */
    // 活跃用户
    val activeUsers = spark.read.table("user_profile.order_index_cleaned").filter("sm_name NOT LIKE '%珠江宽频%'").
                           filter("run_name = '正常'").filter(
      """ offername NOT LIKE '%废%' AND offername NOT LIKE '%空包%' AND
        | offername NOT LIKE '%赠送%' AND offername NOT LIKE '%免费体验%' AND
        | offername NOT LIKE '%提速%' AND offername NOT LIKE '%提价%' AND
        | offername NOT LIKE '%转网优惠%' AND offername NOT LIKE '%测试%' AND
        | offername NOT LIKE '%虚拟%' """.stripMargin
      ).select("phone_no").distinct()
    // 状态正常的用户
    val userMessageRaw = spark.read.table("user_profile.user_message_cleaned")
    val normalUsers = userMessageRaw.filter("run_name = '正常'").
                                    select("phone_no").distinct()
    // 日均收视时长2小时以上的用户
    val addictedUsers = tvAddictionRate.filter("avg_duration >= 2").select("phone_no")
    // 挽留用户
    val keepUsers = activeUsers.intersect(normalUsers).intersect(addictedUsers).withColumn("raw_tag", lit(1))
    // 构造标签列
    val tagColumn = userMessageRaw.select("phone_no").distinct().join(keepUsers, Seq("phone_no"), "left_outer").
                                  withColumn("final_tag", getLabel(col("raw_tag"))).select("phone_no", "final_tag")

    /* 构造完整数据集 */
    val result = tvConsumeLevel.join(tvAccessDuration, "phone_no").join(tvAddictionRate, "phone_no").
                               join(tagColumn, "phone_no")
    // 写入 Hive 数据库
    result.write.mode(Overwrite).saveAsTable("user_profile.svm_dataset")
  }
}
