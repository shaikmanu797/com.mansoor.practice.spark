package com.mansoor.practice.spark.examples

import com.mansoor.practice.spark.logSep
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, udf}

class UDF(implicit val sparkSession: SparkSession) extends Runnable {

  override def getDF: DataFrame = {
    sparkSession.range(1, 20).toDF()
  }

  override def run(): Unit = {
    getDF.createOrReplaceTempView("test")

    val squared = udf((s: Long) => s * s)

    sparkSession.range(1, 20).select(squared(col("id")) as "id_squared").show(truncate = false)

    println(logSep + "Showing output from using registered udf in sql")
    sparkSession.udf.register("squared", squared)
    sparkSession.sql(
      s"""
         |SELECT id, squared(id) as id_squared from test
       """.stripMargin
    ).show(truncate = false)
  }
}
