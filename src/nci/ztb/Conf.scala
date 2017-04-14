package nci.ztb

import com.typesafe.config.ConfigFactory

trait Conf {
  val config = ConfigFactory.load("application.conf")
}