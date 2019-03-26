package com.mansoor.practice.spark.examples

import org.apache.spark.sql.{DataFrame, SparkSession}

class CSV(implicit val sparkSession: SparkSession) extends Runnable {
  override def run(): Unit = {
    val diamondsCSVPath: String = this.getClass.getResource("/input/csv").getPath

    val df: DataFrame = sparkSession.read.option("header", "true").csv(diamondsCSVPath)

    df.explain(true)

    df.printSchema()

    df.show(numRows = 10, truncate = false)
  }
}