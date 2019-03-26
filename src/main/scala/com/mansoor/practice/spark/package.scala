package com.mansoor.practice

import com.mansoor.practice.spark.config.{AppConfig, Resolver}
import org.apache.spark.SparkConf

package object spark {

  val appConfig: AppConfig = Resolver.getConfig

  val conf: SparkConf = new SparkConf()
    .setAppName(appConfig.sparkConfig.appName)
    .setMaster(appConfig.sparkConfig.master)
    .setAll(appConfig.sparkConfig.conf)
}