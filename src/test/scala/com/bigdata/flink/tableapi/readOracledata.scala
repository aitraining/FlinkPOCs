import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.{TableConfig, TableEnvironment}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row


object readOracledata {
  def main(args: Array[String]) {
    /*val rowType = new org.apache.flink.api.java.typeutils.RowTypeInfo(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)*/
    val dec = createTypeInformation[BigDecimal]
    val str = createTypeInformation[String]
    val inte = createTypeInformation[BigInt]
    val rowType = new RowTypeInfo(str,dec,str)

    val jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://learnmysql.cuae8tdqbpef.us-east-1.rds.amazonaws.com:3306/mysqldb")
      .setUsername("musername")
      .setPassword("mpassword")
      .setQuery("select ename, sal, job from emp where sal>2000")
      .setRowTypeInfo(rowType)
      .finish()

    val env = ExecutionEnvironment.getExecutionEnvironment
    val source = env.createInput(jdbcInputFormat)
    //val tableEnv : BatchTableEnvironment= new BatchTableEnvironment(env)
    val tableEnv = BatchTableEnvironment.create(env)

    tableEnv.registerDataSet("customer",source)
    val table = tableEnv.sqlQuery("select * from customer")
    val ds = tableEnv.toDataSet[Row](table)
    ds.print()

  }
}