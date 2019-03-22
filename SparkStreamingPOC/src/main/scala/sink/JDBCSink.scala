package sink

import java.sql._

import org.apache.spark.sql.ForeachWriter

class  JDBCSink(driver: String, url: String, user: String, pwd: String) extends ForeachWriter[org.apache.spark.sql.Row] {
  var connection:Connection = _
  var preparedStmt: PreparedStatement=_

  override def open(partitionId: Long,version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, user, pwd)
    true
  }

  override def process(value: (org.apache.spark.sql.Row)): Unit = {

    println("value Size" + value.size)
    println("value :: " + value)
    println("Value at 0 index :: " + value(0))
    println(value(0).toString.split(",").length)
    var name=value(0).toString.split(",")(0)
    var dep=value(0).toString.split(",")(1)
    var mail=value(0).toString.split(",")(2)

    var sql :String =s"""INSERT INTO public.Employee(NAME,DEPARTMENT,MAIL)
      VALUES (?,?,?)""";

    preparedStmt=connection.prepareStatement(sql)
    preparedStmt.setString(1,name)
    preparedStmt.setString(2,dep)
    preparedStmt.setString(3,mail)
    preparedStmt.execute()
    /*/*statement = connection.createStatement
    statement.execute(s"""INSERT INTO public.Employee(NAME,DEPARTMENT,MAIL)*/
      VALUES ('wq','b','c')""")*/


  }

  override def close(errorOrNull: Throwable): Unit = {
    connection.close
  }
}
