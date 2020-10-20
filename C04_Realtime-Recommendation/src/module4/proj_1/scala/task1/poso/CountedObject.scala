package task1.poso

/**
 * <h1>表数据存储</h1>
 * <p>由于这2张数据表的字段数据类型都是一样的，所以它们可以共用一个POJO。</p>
 *
 * @author Dragon1573
 * */
case class CountedObject(dateTime: String, sale: Double) {}
