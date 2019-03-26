package com.mansoor.practice.spark

import org.apache.spark.sql.SparkSession

object RunApp extends App {

  implicit lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  println(sparkSession.conf.getAll)

//  sparkSession.catalog.listDatabases().show(false)
}
