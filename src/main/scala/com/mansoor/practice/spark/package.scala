package com.mansoor.practice

import java.io.File
import com.mansoor.practice.spark.config.{AppConfig, Resolver}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

package object spark {

  val logSep: String = "#"*25

  val sparkPropsResource: File = new File(this.getClass.getResource("/spark.properties").getPath)

  val appConfig: AppConfig = Resolver.getConfig

  var sparkConf: SparkConf = _
  implicit var sparkSession: SparkSession = _
}