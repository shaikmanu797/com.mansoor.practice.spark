package com.mansoor.practice.spark.config

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConverters._

object Resolver {

  def getConfig: AppConfig = {
    ConfigFactory.invalidateCaches()

    val systemConfig: Config = ConfigFactory.systemProperties()
      .withFallback(ConfigFactory.systemEnvironment())

    val appConfig: Config = ConfigFactory.load()
      .resolveWith(systemConfig)
      .getConfig("practice")

    def getSparkConf: SparkConfig = {
      val sparkConf: Config = appConfig.getConfig("sparkConf")

      val appName: String = sparkConf.getString("appName")
      val master: String = sparkConf.getString("master")
      val conf: Map[String, String] = sparkConf.getStringList("conf").asScala
        .map(s => {
          val item: Array[String] = s.split("=", 2)
          item(0) -> item(1)
        })
        .toMap

      SparkConfig(appName, master, conf)
    }

    AppConfig(getSparkConf)
  }
}
