package stage3

import stage1.ResearchTarget.analysis
import stage1.ResearchTarget.loadDatabase

/**
 * <h1>需求三：主要状态用户探索</h1>
 *
 * @author Dragon1573
 * */
object MajorStatus {
  def main(args: Array[String]): Unit = {
    val database = loadDatabase("主要状态用户探索")
    Array("user_messages", "user_events", "order_index").foreach(table => {
      println(s"数据表 `$table`: ")
      analysis(database(table), "run_name")
    })
  }
}
