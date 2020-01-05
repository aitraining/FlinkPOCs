package com.bigdata.flink.tableapi

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSinkBuilder
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.api.scala.typeutils.Types._
import org.apache.flink.table.api.Table
import org.apache.flink.table.sinks._
import org.apache.flink.table.utils._
//import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestSinkUtil, TestingAppendTableSink, TestingRetractTableSink}
import org.apache.flink.sql.parser
import org.apache.flink.sql.parser._
import org.apache.flink.sql.parser.ddl.SqlCreateTable
import java.sql.Timestamp

object writeOracleData {
  case class aslcc(name:String,age:Int, city:String)
  val str = createTypeInformation[String]
  val dec = createTypeInformation[Int]
  val dt: TypeInformation[Timestamp] = createTypeInformation[Timestamp]
  var inte: TypeInformation[BigInt] = createTypeInformation[BigInt]



  val DB_ROWTYPE = new RowTypeInfo(inte,str,str,inte,dt,inte,inte,inte)

  val rowType = new RowTypeInfo(str,str,str)
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)
    val data = "C:\\work\\datasets\\asl\\asl.csv"
    val ds = env.readCsvFile[aslcc](data,ignoreFirstLine = true)
    ds.print()
    val tab = tEnv.fromDataSet(ds)
    tEnv.registerTable("asl", tab)
    val query = "select * from asl"
    val res = tEnv.sqlQuery(query)
    val jdbcSink =JDBCAppendTableSink.builder()
      .setDrivername("com.mysql.jdbc.Driver")
      .setDBUrl("jdbc:mysql://learnmysql.cuae8tdqbpef.us-east-1.rds.amazonaws.com:3306/mysqldb")
      .setUsername("musername")
      .setPassword("mpassword")
      .setQuery("insert into asltab (name,age,city) values(?,?,?)")
      .setParameterTypes( Types.STRING,Types.INT, Types.STRING)
      .build()

    val fn = Array("name", "age", "city")
    val ft= Array(Types.STRING, Types.INT, Types.STRING)

// please note before that creaet a table in advanced in mysql
    //add mysql driver as dependency
    //create table asltab(name varchar(32), age int, city varchar(32))

    tEnv.registerTableSink(
      "ihr_register",
      // specify table schema
      Array[String]("name", "age", "city"),
      Array[TypeInformation[_]](STRING, INT, STRING),
      jdbcSink)

    res.insertInto("ihr_register")

    env.execute()

  }
}
//https://github.com/xiaoyan5686670/safe_flink/blob/bccfc5b7b8b90debd9bbd1d97ce81b3405eebedf/src/main/scala/Safert.scala
