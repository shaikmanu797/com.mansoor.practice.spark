package com.mansoor.practice.spark.examples

import com.mansoor.practice.spark.logSep
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class DFBuilder(implicit val sparkSession: SparkSession) extends Runnable {

  var data: Seq[(Int, String)] = Seq(
    (1, "Mansoor"),
    (2, "Rob"),
    (3, "Aron"),
    (4, "Bobby")
  )

  val data1: Seq[Row] = data.map(x => Row(x._1, x._2))

  var schema: StructType = StructType(
    List(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false)
    )
  )

  override def getDF: DataFrame = {
    import sparkSession.implicits._
    data.toDF(schema.fieldNames:_*)
  }

  override def run(): Unit = {
    getDF.explain(true)

    getDF.cache()

    getDF.printSchema()

    getDF.show(numRows = 10, truncate = false)

    println(logSep + "Showing output from RDD based DF")

    sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(data1),
      schema
    ).show(numRows = 10, truncate = false)
  }
}
