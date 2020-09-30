package chapter3.part2

import org.apache.spark.sql.functions.{count, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("ͳ��������������������")
      .getOrCreate()
    val data: DataFrame = spark.sql("SELECT * FROM law.law_visit_log_all")
    //ͳ���շ�������
    data.select("ymd").groupBy("ymd")
        .agg(count(lit(1)) as "day_visits")
        .orderBy("ymd").show(10)
    // ͳ�����û�����
    data.select("ymd", "userid")
        .distinct.groupBy("ymd")
        .agg(count(lit(1)) as "day_users")
        .orderBy("ymd").show(10)
  }
}
