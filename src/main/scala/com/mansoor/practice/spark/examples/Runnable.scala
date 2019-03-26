package com.mansoor.practice.spark.examples

import org.apache.spark.sql.DataFrame

trait Runnable {
  def getDF: DataFrame
  def run(): Unit
}
