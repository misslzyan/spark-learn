package com.dwd.scala

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by xiaoyanzi on 18/12/26.
  */
object SparkSqlStuctType {

  def main(args: Array[String]): Unit = {
    import SparkSessionF._
    val rows = sc.textFile(s"${path}/person.txt").map(_.split(","))
      .map(attributes=>Row(attributes(0),attributes(1)))
    val schemaS = "name,age"
    val fileds = schemaS.split(",").map(StructField(_,StringType,nullable = true))
    val schema = StructType(fileds)
    val df = spark.createDataFrame(rows,schema)
    df.createOrReplaceTempView("people")
    val res= spark.sql("select * from people")
    res.show()
  }

}
