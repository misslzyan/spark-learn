package com.dwd.scala

/**
  * Created by xiaoyanzi on 18/12/26.
  */
object SparkSql3 {

  def main(args: Array[String]): Unit = {
    import SparkSessionF._
    import spark.implicits._
    val p = sc.textFile(s"${path}/person.txt",1).map(_.split(",")).map(row=>Person(row(0),row(1).trim.toLong)).toDF()
    p.show()
    p.createOrReplaceTempView("people")
    val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 10 AND 19")
    teenagersDF.show()
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    // Primitive types and case classes can be also defined as
    // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    val col = teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    col.foreach(println(_))
    sc.stop()
  }

}
