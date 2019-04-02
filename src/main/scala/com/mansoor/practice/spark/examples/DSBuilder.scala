package com.mansoor.practice.spark.examples

import org.apache.spark.sql.{SparkSession, Dataset}

case class Person(index: Int,
                  age: Int,
                  name: String,
                  gender: String,
                  company: String,
                  email: String,
                  phone: String,
                  address: String
                 )

case class Persons(details: Array[Person])

class DSBuilder(implicit val sparkSession: SparkSession) extends Runnable {

  var filePath: String = this.getClass.getResource("/input/json").getPath

  def getDS: Dataset[Person] = {
    import sparkSession.implicits._

    sparkSession.read.json(filePath).as[Person]
  }

  override def run(): Unit = {
    sparkSession.read.json(filePath).take(2)
//    getDS.show(20, truncate = false)
  }
}
