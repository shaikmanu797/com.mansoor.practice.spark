package com.mansoor.practice.spark.examples

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class CSV(implicit val sparkSession: SparkSession) extends Runnable {
  var filePath: String = this.getClass.getResource("/input/csv").getPath
  var options: Map[String, String] = Map (
    "header" -> "true"
  )
  var schema: StructType = StructType.fromDDL(
    """
      |carat double,
      |cut string,
      |color string,
      |clarity string,
      |depth double,
      |table int,
      |price int,
      |x double,
      |y double,
      |z double
    """.stripMargin
  )

  override def getDF: DataFrame = {
    sparkSession.read.options(options).schema(schema).csv(filePath)
  }

  override def run(): Unit = {

    getDF.explain(true)

    getDF.cache()

    getDF.printSchema()

    getDF.show(numRows = 10, truncate = false)

    getDF.write.mode(SaveMode.Overwrite).partitionBy("cut").saveAsTable("outcsv")
  }
}