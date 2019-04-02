package com.mansoor.practice.spark.config

object Examples extends Enumeration {
  type Example = Value
  val csv, df, udf, ds: Example = Value
}