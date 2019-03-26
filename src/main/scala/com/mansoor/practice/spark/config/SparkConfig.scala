package com.mansoor.practice.spark.config

final case class SparkConfig(appName: String, master: String, conf: Map[String, String])