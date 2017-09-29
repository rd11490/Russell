package com.rrdinsights.russell.utils

import com.typesafe.scalalogging
import com.typesafe.scalalogging.slf4j.Logger

import scala.language.implicitConversions

trait RussellLogger extends scalalogging.Logging {

  override protected val logger: Logger =
    Logger(org.slf4j.LoggerFactory.getLogger(getClass.getName))

}
