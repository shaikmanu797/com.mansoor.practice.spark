package com.mansoor.practice.spark.config

import java.io.File
import com.mansoor.practice.spark.config.Examples.Example
import com.mansoor.practice.spark.sparkPropsResource

final case class RuntimeConfig(
                                example: Example = Examples.csv,
                                sparkPropertiesFile: File = sparkPropsResource,
                                run: Boolean = false
                              )