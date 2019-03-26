package com.mansoor.practice.spark

import com.mansoor.practice.spark.config.{Examples, RuntimeConfig}
import java.io.File
import com.mansoor.practice.spark.examples.CSV
import org.apache.spark.SparkConf
import scopt.OParser
import org.apache.spark.sql.SparkSession
import scala.io.Source

object RunApp extends App {
  implicit val examples: scopt.Read[Examples.Value] = scopt.Read.reads(Examples.withName)

  val builder = OParser.builder[RuntimeConfig]
  val parser = {
    import builder._
    OParser.sequence(
      programName("Spark Practice Examples"),
      head("scopt", "4.x"),
      opt[Examples.Value]('e', "example")
          .valueName("<exampleType>")
          .required()
          .action((x, c) => c.copy(example = x))
          .text("example is a required property"),
      opt[File]('s', "sparkPropertiesFile")
        .valueName("<file>")
        .optional()
        .action((x, c) => c.copy(sparkPropertiesFile = x))
        .text("sparkPropertiesFile is an optional file property"),
      opt[Unit]('r', "run")
        .required()
        .action((x, c) => c.copy(run = true))
        .text("run is a required flag to execute the example"),
      note(sys.props("line.separator"))
    )
  }

  OParser.parse(parser, args, RuntimeConfig()) match {
    case Some(config) =>
      val conf: Map[String, String] = Source.fromFile(config.sparkPropertiesFile)
        .getLines()
        .filter(_.nonEmpty)
        .map(s => {
          val item: Array[String] = s.split("=", 2)
          item(0) -> item(1)
        })
        .toMap

      sparkConf = new SparkConf()
        .setAppName(appConfig.sparkConfig.appName)
        .setMaster(appConfig.sparkConfig.master)
        .setAll(conf)

      sparkSession = SparkSession
        .builder()
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()

      sparkSession.sparkContext.setLogLevel("WARN")

      config.example match {
        case Examples.csv => new CSV().run()
        case _ => println("Unsupported example!")
      }
    case _ =>
    // arguments are bad, error message will have been displayed
  }
}