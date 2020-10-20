package task6.stage2

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.functions.substring_index
import org.apache.spark.sql.types.DoubleType

/**
 * <h1>特征构建</h1>
 * <p>暂不支持 IntelliJ IDEA 直接运行，请使用 spark-submit 提交任务。</p>
 *
 * @author Dragon1573
 * */
object FeatureConstruct {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("spark://master:7077").appName("Feature-Constructing").
                            config("hive.metastore.uris", "thrift://master:9083").enableHiveSupport().
                            getOrCreate()

    /* 加载数据集 */
    val data = spark.read.table("advertisement.case_data_sample_new")

    /* 获取时间戳最值 */
    val timestampRange = data.select(max("timestamps") as "max_ts", min("timestamps") as "min_ts").rdd.collect()
    val maxTimeStamp = timestampRange(0).getString(0).toInt
    val minTimeStamp = timestampRange(0).getString(1).toInt

    /* 产生时间片（5小时/片）*/
    val timeSlice = List.range(minTimeStamp, maxTimeStamp, 5 * 3600)

    /* 计算每个时间片的特征并合并 */
    timeSlice.indices.foreach{ i =>
      // 提取时间片中的数据
      val dataSubset = {
        if (i == timeSlice.length - 1) {
          data.filter(s"timestamps >= ${timeSlice(i)}")
        } else {
          data.filter(s"timestamps >= ${timeSlice(i)} AND timestamps < ${timeSlice(i + 1)}")
        }
      }

      // N1特征合并
      val dataN1Subset = dataSubset.groupBy("cookie", "ip").agg(count("ip") as "N1").
                                   join(dataSubset, Seq("cookie", "ip"), "inner").
                                   select("rank", "N1")

      // N2特征合并
      val dataN2Subset = dataSubset.groupBy("ip").agg(countDistinct("cookie") as "N2").
                                   join(dataSubset, Seq("ip"), "inner").
                                   select("rank", "N2")

      // N3特征合并
      val dataIpDual = dataSubset.withColumn("ip_dual", substring_index(col("ip"), ".", 2))
      val dataN3Subset = dataIpDual.groupBy("ip_dual").agg(count("ip_dual") as "N3").
                                   join(dataIpDual, Seq("ip_dual"), "inner").
                                   select("rank", "N3")

      // N4特征合并
      val dataIpTriple = dataSubset.withColumn("ip_triple", substring_index(col("ip"), ".", 3))
      val dataN4Subset = dataIpTriple.groupBy("ip_triple").agg(count("ip_triple") as "N4").
                                     join(dataIpTriple, Seq("ip_triple"), "inner").
                                     select("rank", "N4")

      // 特征合并，暂存至 Hive
      dataN1Subset.join(dataN2Subset, "rank").join(dataN3Subset, "rank").join(dataN4Subset, "rank").
                  repartition(3).write.mode(SaveMode.Append).saveAsTable("advertisement.case_data_sample_model_n")
    }

    // 标签类型转换，覆盖 Hive
    spark.read.table("advertisement.case_data_sample_model_n").join(data, "rank").
         select(col("rank"), col("N1"), col("N2"), col("N3"), col("N4"), col("label").cast(DoubleType)).
         repartition(3).write.mode(SaveMode.Overwrite).saveAsTable("advertisement.case_data_sample_model")
  }
}
