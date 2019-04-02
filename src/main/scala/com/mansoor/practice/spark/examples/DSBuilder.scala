package com.mansoor.practice.spark.examples

import org.apache.spark.sql.{SparkSession, Dataset}

case class Person(index: Long,
                  age: Long,
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

    sparkSession.read.option("multiLine", "true").json(filePath).as[Person]
  }

  override def run(): Unit = {
    getDS.explain(true)

    getDS.cache()

    getDS.printSchema()

    getDS.show(numRows = 10, truncate = false)
  }
}
