package sink

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import poso.StoreSaleRecord

/**
 * <h1>MySQL数据输出池</h1>
 *
 * @author Dragon1573
 * */
class MysqlSink extends RichSinkFunction[StoreSaleRecord] with Serializable {
  override def invoke(value: StoreSaleRecord): Unit = {
    val jdbcDriver = "com.mysql.jdbc.Driver"
    Class.forName(jdbcDriver)

    val jdbcURL = "jdbc:mysql://master:3306/shop"
    val connection: Connection = DriverManager.getConnection(jdbcURL, "root", "123456")

    val statement: PreparedStatement = connection.prepareStatement("INSERT INTO store_sale_daily VALUES (?, ?, ?)")
    statement.setString(1, value.dateTime)
    statement.setString(2, value.storeId)
    statement.setDouble(3, value.saleVolume)
    statement.executeUpdate()

    if (statement != null) statement.close()
    if (connection != null) connection.close()
  }
}
