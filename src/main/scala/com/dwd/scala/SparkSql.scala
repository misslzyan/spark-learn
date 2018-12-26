package com.dwd.scala

import org.apache.spark.sql.SparkSession

/**
  * Created by xiaoyanzi on 18/12/25.
  */
object SparkSql {

  val prefix = "src/main/resources/"


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
        .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val df = spark.read.json(s"${prefix}people.json")
    df.createTempView("people")
    val res = df.sqlContext.sql("select * from people")
    import spark.implicits._
    res.printSchema
    res.filter($"age">21).show()
    res.select("name").show()
    res.groupBy("age").count().show()



  }
}
