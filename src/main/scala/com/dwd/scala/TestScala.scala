package com.dwd.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xiaoyanzi on 18/12/24.
  */
object TestScala {

  def main(args: Array[String]): Unit = {
    val r = Seq(1,2,3)
    r.foreach(println(_))
    val conf = new SparkConf
    conf.setAppName("test")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val path = "/Users/xiaoyanzi/Documents/code/dwd/spark-learn"
    sc.makeRDD(Seq(1,2,3,4)).saveAsTextFile(path+"/1")
    sc.stop()
  }
}
class TestScala{}
