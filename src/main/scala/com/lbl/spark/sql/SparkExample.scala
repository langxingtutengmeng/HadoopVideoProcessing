package com.lbl.spark.sql

import org.apache.spark.sql.SparkSession

/**
  * Created by 22731 on 2017/11/9.
  */
object SparkExample {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Test").master("local[2]").getOrCreate()
    /*val url = "jdbc:mysql://localhost:3306/tmp"
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "yimeng1219")
    val df = spark.read.jdbc(url, "scores", prop)
    df.show()
    df.printSchema()
    df.createOrReplaceTempView("user")
    val sqlDF = spark.sql("SELECT * FROM user")
    sqlDF.show()*/

    /*import spark.implicits._
    case class Person(name: String, age: Long)
    val caseClassDS = Seq(Person("Any", 32)).toDS
    caseClassDS.show()*/

    val df = spark.read.json("D:\\Application\\spark-2.1.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")
  }
}
