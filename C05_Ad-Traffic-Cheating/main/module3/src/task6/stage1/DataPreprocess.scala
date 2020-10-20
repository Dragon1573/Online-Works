package task6.stage1

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

/**
 * <h1>数据预处理</h1>
 * <p>此脚本编写貌似有点问题，必须通过 spark-submit 提交至集群运行。暂不支持使用 IntelliJ IDEA 运行！</p>
 *
 * @author Dragon1573
 */
object DataPreprocess {
  def main(args: Array[String]): Unit = {
    // 获取 SparkSession 对象
    val spark = SparkSession.builder().master("spark://master:7077").appName("Data-Preprocessing").
                            config("hive.metastore.uris", "thrift://master:9083").enableHiveSupport().
                            getOrCreate()

    /* 数据预处理 */
    spark.read.table("advertisement.case_data_sample").
         drop("mac").drop("creativeid").drop("mobile_os").drop("mobile_type"). // 删除无效列（一）
         drop("app_key_md5").drop("app_name_md5").drop("os_type"). // 删除无效列（二）
         write.mode(SaveMode.Overwrite).saveAsTable("advertisement.case_data_sample_new") // 写入 Hive 数据表
  }
}
