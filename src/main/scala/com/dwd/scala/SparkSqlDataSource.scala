package com.dwd.scala

/**
  * Created by xiaoyanzi on 18/12/26.
  */
object SparkSqlDataSource {

  def main(args: Array[String]): Unit = {
    import SparkSessionF._
    val users = spark.read.format("parquet").load(s"${path}/users.parquet")
    users.write.format("json").save(s"${path}/users_json")
  }

}
