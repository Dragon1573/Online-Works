import org.apache.spark.SparkContext

import scala.util.Random

/**
 * <h1>用户日志随机生成器</h1>
 *
 * @author Dragon1573
 * */
object QueryGenerator {
  /* 获取随机字符串 */
  def getRandomStr(length: Int): String = Random.alphanumeric.take(length).map(_.toString).reduce(_ + _)

  /* 性别列表 */
  val gender = Array("Male", "Female")

  /* 用户行为 */
  val action = Array("pv", "buy", "cart", "fav", "scan")

  /* 随机手机号码（3位运营商前缀＋4位尾号） */
  val prefix = Array(
    "130", "131", "132", "133", "134", "135", "136", "137", "138", "139", "147", "150",
    "151", "152", "153", "155", "156", "157", "158", "159", "186", "187", "188"
    )

  /* 随机邮箱地址 */
  val suffix = Array("163.com", "126.com", "qq.com", "gmail.com", "huawei.com")

  /* 随机生成一条用户日志 */
  def getRandomLog(rowKey: Long): String = {
    val randomStr = getRandomStr(10)
    val hashCode = randomStr.hashCode
    String.join(
      ",", rowKey.toString, // 行索引号
      randomStr.take(5).toLowerCase, // 用户名
      (18 + (randomStr.hashCode % 43)).toString, // 年龄
      gender(hashCode % 2), // 性别
      (hashCode % 12).toString, // 商品编号
      hashCode.toString, // 商品价格
      (hashCode % 12).toString, // 商户编号
      action(hashCode % 5), // 用户行为
      prefix(hashCode % 23) + (hashCode % 10000).formatted("%04d"), // 手机号码
      s"$randomStr@${suffix(hashCode % 5)}", // 邮箱地址
      s"2019-08-0${1 + (hashCode % 7)}" // 交易日
      )
  }

  def main(args: Array[String]): Unit = {
    println(args.mkString("Array(", ", ", ")"))
    if (args.length != 1)
      sys.error("用法：spark-submit --class QueryGenerator *.jar <数据量>")
    else {
      val sparkContext = new SparkContext("spark://master:7077", "用户日志生成")
      val dataset = (1 to args(0).toInt).map(getRandomLog(_))
      sparkContext.parallelize(dataset).
                  saveAsTextFile("hdfs://master:8020/user/hadoop/raw")
    }
  }
}
