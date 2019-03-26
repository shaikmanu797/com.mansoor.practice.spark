package com.mansoor.practice.spark.config

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConverters._
import scala.io.Source

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

      SparkConfig(appName, master)
    }

    AppConfig(getSparkConf)
  }
}
