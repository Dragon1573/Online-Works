package task1.mysql_sink

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import task1.poso.CountedObject

/**
 * <h1>销售数据存储池</h1>
 *
 * @author Dragon1573
 * */
class SaleMysqlSink(tableName: String) extends RichSinkFunction[CountedObject] with Serializable {
  override def invoke(value: CountedObject): Unit = {
    /* 加载驱动器 */
    val jdbcDriver = "com.mysql.jdbc.Driver"
    Class.forName(jdbcDriver)

    /* 连接数据库 */
    val url = "jdbc:mysql://master:3306/shop"
    val userName = "root"
    val password = "123456"
    val connection: Connection = DriverManager.getConnection(url, userName, password)

    /* 创建SQL语句 */
    val statement: PreparedStatement = connection.prepareStatement(s"INSERT INTO $tableName VALUES (?, ?);")
    statement.setString(1, value.dateTime)
    statement.setDouble(2, value.sale)

    /* 执行语句 */
    statement.executeUpdate()

    /* 断开数据库连接 */
    if (statement != null) statement.close()
    if (connection != null) connection.close()
  }
}
