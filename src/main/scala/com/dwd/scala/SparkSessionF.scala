package com.dwd.scala

import org.apache.spark.sql.SparkSession

/**
  * Created by xiaoyanzi on 18/12/26.
  */
object SparkSessionF {

  val spark=SparkSession.builder().master("local").appName("sql").getOrCreate()

  import SparkSql._
  val path = prefix

  val sc = spark.sparkContext

}

