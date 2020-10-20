package task7

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

/**
 * <h1>数据标准化</h1>
 *
 * @author Dragon1573
 * */
object DataStandardize {
  def main(args: Array[String]): Unit = {
    // Spark Session 对象
    val spark = SparkSession.builder().master("spark://master:7077").appName("Data Standardize").
                            config("hive.metastore.uris", "thrift://master:9083").enableHiveSupport().
                            getOrCreate()

    // 加载数据
    val data = spark.read.table("advertisement.case_data_sample_model")

    /* 使用机器学习流水线标准化数据 */
    val vectorAssembler = new VectorAssembler().setInputCols(Array("N1", "N2", "N3", "N4")).
                                               setOutputCol("features")
    val minMaxScaler = new MinMaxScaler().setInputCol("features").setOutputCol("scaled_features")
    val scaledData = new Pipeline().setStages(Array(vectorAssembler, minMaxScaler)).fit(data).transform(data)

    // 展示标准化数据
    scaledData.show(false)
  }
}
