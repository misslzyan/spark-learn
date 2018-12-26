package com.dwd.scala

import org.apache.spark.sql.SparkSession

/**
  * Created by xiaoyanzi on 18/12/26.
  */
object SaprkSql2 {

  def main(args: Array[String]): Unit = {
   val spark =  SparkSession.builder().master("local").appName("testSql").getOrCreate()
    import  spark.implicits._
    val pDs = Seq(Person("andy",12)).toDS()
    pDs.show()
    import SparkSql._
    spark.read.json(prefix+"people.json").as[Person].show()
  }

}
