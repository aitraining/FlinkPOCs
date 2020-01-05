package com.bigdata.flink.tableapi
import org.apache.flink.table.api.scala.BatchTableEnvironment

import org.apache.flink.api.scala._
import org.apache.flink.table.api._

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object flink_read_csv {
  case class aslcc(name:String,age:Integer, city:String)
  def main(args: Array[String]): Unit = {


    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

     val data = "C:\\work\\datasets\\asl\\asl.csv"
    val ds = env.readCsvFile[aslcc](data,ignoreFirstLine = true)
    //ds.print()
    val tab = tEnv.fromDataSet(ds)
    tEnv.registerTable("asl", tab)
    val query = "select * from asl where city='Bangalore'"
    val result = tEnv.sqlQuery(query)
    val resultData = tEnv.toDataSet[aslcc](result)
    resultData.print()
  }


}
